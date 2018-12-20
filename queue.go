package diskqueue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TIMETOFLUSH = 1e9
	MAXSIZE     = 100000000

	HEADERSZ = 22
	DELRECSZ = 43
	constBKP = ".bkp"
)

// Data Struct
// Contém os registros do Push
type Data struct {
	Offset  uint32
	Payload []byte
}

//Msg append only file system
//Length +  CRC(payload)    + Payload
//int32 +   byte        +   int32 + n bytes
//(1+4+n)

// Timestamp into payload
// In-memory control active segments
type Record struct {
	Len     int32
	Crc     uint32
	Payload []byte
}

// uint32 + int64 (4+8) = 12 bytes
// offset crc + offset last pop record
type RecordPop struct {
	Crc     uint32 //10 + space
	Offset  int64  //20 + space
	CrcPush uint32 //10 + \n
}

type DataChannelResponse struct {
	Response []byte
	Err      error
}

type DiskQueue struct {
	sdir string

	currFilePush     int
	currFilePushRead int

	pushRecNro   uint64 // Current number of records
	pushFileSize int64  // Current file size
	pushFH       *os.File

	pushReadNro    uint64 // Push file name for read
	pushReadFH     *os.File
	pushReadOffset int64

	popRecNro uint64 // Current number of records
	popFH     *os.File

	mutex sync.RWMutex

	maxsize uint64

	loading      bool
	clearStarted bool

	sleep time.Time

	dataChannel         chan queueDataChan
	dataChannelResponse chan queueDataChanResponse
}

type queueDataChan struct {
	Command string
	Param   []byte
}

type queueDataChanResponse struct {
	Response []byte
	Err      error
}

func New(dbdir string) (dq *DiskQueue, err error) {
	dq = &DiskQueue{
		sdir:                dbdir,
		maxsize:             MAXSIZE,
		loading:             true,
		dataChannel:         make(chan queueDataChan),
		dataChannelResponse: make(chan queueDataChanResponse),
	}

	// Cria os diretorios para armazenar so arquivos de fila
	err = dq.createDBDir()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro MkdirAll %s", err))
	}

	err = dq.prepareFiles()
	if err != nil {
		return
	}

	fhInfo, err := dq.pushFH.Stat()
	if err != nil {
		return nil, err
	}

	dq.pushFileSize = fhInfo.Size()

	dq.pushReadOffset, err = dq.ReadDelRecord()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Erro Last record %s", err))
	}

	go dq.flush()
	go dq.deliver()
	dq.sleep = time.Now().UTC()

	return dq, nil
}

func (dq *DiskQueue) deliver() {

	for s := range dq.dataChannel {

		switch s.Command {
		case "push":
			err := dq.push(s.Param)
			dcr := queueDataChanResponse{nil, err}
			dq.dataChannelResponse <- dcr

		case "pop":
			res, err := dq.pop()
			dcr := queueDataChanResponse{res, err}
			dq.dataChannelResponse <- dcr

		default:
		}
	}
}

func (dq *DiskQueue) Pop() ([]byte, error) {
	d := queueDataChan{"pop", nil}
	dq.dataChannel <- d
	dcr := <-dq.dataChannelResponse
	return dcr.Response, dcr.Err
}

func (dq *DiskQueue) Push(s []byte) error {
	d := queueDataChan{"push", s}
	dq.dataChannel <- d
	dcr := <-dq.dataChannelResponse
	return dcr.Err
}

func (dq *DiskQueue) createDBDir() error {
	//Dir structures
	err := os.MkdirAll(dq.sdir, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro MkdirAll dir: %s", err))
	}

	//Dir .bkp
	err = os.MkdirAll(filepath.Join(dq.sdir, constBKP), 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro MkdirAll .bkp: %s", err))
	}
	return nil
}

func (dq *DiskQueue) prepareFiles() (err error) {
	err = dq.openToWrite()
	if err != nil {
		return
	}

	err = dq.openToRead()
	if err != nil {
		return
	}
	return
}

func (dq *DiskQueue) lastFilePushRead() (path string) {
	path = "push"

	files, err := ioutil.ReadDir(dq.sdir)
	if err != nil {
		return
	}

	minor := math.MaxUint32
	for _, file := range files {
		if skipFile(file) {
			continue
		}
		num, ok := getFileNameInt(file.Name())
		if !ok {
			return
		}
		if num < minor {
			minor = num
		}
	}
	if minor > 0 && minor != math.MaxUint32 {
		path = fmt.Sprintf("push_%d", minor)
		return
	}
	return
}

func (dq *DiskQueue) lastFilePushWrite() (path string) {
	path = "push"
	if dq.loading {
		dq.loading = false

		files, err := ioutil.ReadDir(dq.sdir)
		if err != nil {
			return
		}
		bigger := 0
		for _, file := range files {
			if skipFile(file) || file.Name() == "push" {
				continue
			}
			num, ok := getFileNameInt(file.Name())
			if !ok {
				return
			}
			if num > bigger {
				bigger = num
			}
		}
		if bigger > 0 {
			dq.currFilePush = bigger
			path = fmt.Sprintf("push_%d", bigger)
			return
		}
	} else {
		if dq.currFilePush > 0 {
			path = fmt.Sprintf("push_%d", dq.currFilePush)
		}
	}

	return
}

func skipFile(file os.FileInfo) bool {
	name := file.Name()
	if file.IsDir() || name == "pop" ||
		strings.Contains(name, ".") ||
		strings.HasSuffix(name, "gz") ||
		strings.HasSuffix(name, "bkp") ||
		strings.HasSuffix(name, "zip") ||
		strings.HasSuffix(name, "log") ||
		strings.HasSuffix(name, "txt") {
		return true
	}

	return false
}

func getFileNameInt(name string) (num int, ok bool) {
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return
	}
	num, err := strconv.Atoi(parts[1])
	if err != nil {
		return
	}
	ok = true
	return
}

func (dq *DiskQueue) MaxSize(m uint64) {
	dq.maxsize = m
}

func (dq *DiskQueue) getFileNameToRead() string {
	return dq.lastFilePushRead()

}

func (dq *DiskQueue) getFileNameToWrite() string {
	return dq.lastFilePushWrite()
}

// Abre o arquivo para escrever
func (dq *DiskQueue) openToWrite() error {

	var err error

	sdirpush := filepath.Join(dq.sdir, dq.getFileNameToWrite())

	dq.pushFH, err = os.OpenFile(sdirpush, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro Open push %s", err))
	}

	return nil
}

func (dq *DiskQueue) openToRead() (err error) {
	filePush := dq.getFileNameToRead()

	dq.pushReadFH, err = os.OpenFile(filepath.Join(dq.sdir, filePush), os.O_RDONLY, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro Open push read %s", err))
	}
	sdirpop := filepath.Join(dq.sdir, "pop")
	dq.popFH, err = os.OpenFile(sdirpop, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return errors.New(fmt.Sprintf("Erro Open pop %s", err))
	}

	return
}

func (dq *DiskQueue) closeWriter() error {
	if dq.pushFH != nil {
		return dq.pushFH.Close()
	}
	return nil
}

func (dq *DiskQueue) closeReader() (err error) {
	if dq.pushReadFH != nil {
		err = dq.pushReadFH.Close()
	}
	if dq.popFH != nil {
		err = dq.popFH.Close()
	}
	return nil
}

//Disk Storage Close
func (dq *DiskQueue) Close() (err error) {
	if dq.pushFH != nil {
		err = dq.pushFH.Close()
	}
	if dq.pushReadFH != nil {
		err = dq.pushReadFH.Close()
	}
	if dq.popFH != nil {
		err = dq.popFH.Close()
	}

	return err
}

//Flush Disk
func (dq *DiskQueue) flush() {
	for {
		time.Sleep(TIMETOFLUSH)
		dq.mutex.Lock()
		dq.popFH.Sync()
		dq.pushFH.Sync()
		dq.mutex.Unlock()
	}
}

func sliceCRC(data []byte) uint32 {
	buff := new(bytes.Buffer)
	buff.Write(data)
	return crc32.ChecksumIEEE(buff.Bytes())
}

func int64CRC(data int64) uint32 {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, data)
	return crc32.ChecksumIEEE(buff.Bytes())
}

func (dq *DiskQueue) createNewWriteFile() (err error) {
	dq.closeWriter()
	dq.currFilePush++
	dq.openToWrite()

	//Zera Ponteiros
	dq.pushFileSize = 0
	dq.pushRecNro = 0
	dq.pushReadNro = 0
	return
}

//PUSH =================================================================
//Write a data into record
func (dq *DiskQueue) push(data []byte) (err error) {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	// Aqui deverá ser verificado se o arquivo chegou no tamanho
	if uint64(dq.pushFileSize) > dq.maxsize {
		dq.createNewWriteFile()
	}
	// Acrescentar  no final uma quebra de linha \n
	data = append(data, []byte("\n")...)

	rec, err := buildPushRecord(data)
	if err != nil {
		return
	}
	total := 0
	total, err = dq.pushRecord(rec)
	if err != nil {
		return
	}
	dq.pushFileSize += int64(total)

	dq.pushRecNro++

	return
}

func buildPushRecord(data []byte) (r *Record, err error) {
	//payload length
	sz := int32(len(data))
	if sz == 0 {
		err = errors.New("Data is empty")
		return
	}
	//Calculating CRC32
	crc := sliceCRC(data)
	r = &Record{sz, crc, data}
	return
}

func (dq *DiskQueue) pushRecord(r *Record) (int, error) {
	buff := new(bytes.Buffer)
	//More readable (22 bytes)
	buff.Write([]byte(fmt.Sprintf("%010d", r.Len)))
	buff.Write([]byte(" "))
	buff.Write([]byte(fmt.Sprintf("%010d", r.Crc)))
	buff.Write([]byte(" "))
	buff.Write(r.Payload)
	total, err := dq.pushFH.Write(buff.Bytes())
	return total, err
}

type RecData struct {
	Data []byte
}

func (dq DiskQueue) getAllReadFiles() ([]string, error) {
	files, err := ioutil.ReadDir(dq.sdir)
	if err != nil {
		return nil, err
	}
	var fileItems []string
	var fileNumbers []int
	for _, file := range files {
		if skipFile(file) || !strings.HasPrefix(file.Name(), "push") {
			continue
		}
		num, ok := getFileNameInt(file.Name())
		if ok {
			fileNumbers = append(fileNumbers, num)
		} else {
			fileItems = append(fileItems, file.Name())
		}
	}
	sort.Ints(fileNumbers)
	for _, num := range fileNumbers {
		fileItems = append(fileItems, fmt.Sprintf("push_%d", num))
	}
	return fileItems, nil
}

//POP READ ONLY ========================================================
func (dq *DiskQueue) PopReadOnly(nroRec uint64) (data [][]byte, nroRecDone uint64) {
	dq.mutex.Lock()
	defer func(nrec *uint64) {
		if nroRec != 0 && uint64(dq.pushReadOffset) > dq.maxsize && *nrec == 0 {
			if errRotate := dq.rotatePop(); errRotate != nil {
				fmt.Printf("rotate fail: %s\n\tread: %s\n\twrite: %s\n", errRotate.Error(), dq.pushReadFH.Name(), dq.pushFH.Name())
				dq.mutex.Unlock()
				return
			}
		}
		dq.mutex.Unlock()
	}(&nroRecDone)

	dq.pushReadFH.Seek(dq.pushReadOffset, os.SEEK_SET)
	files, err := dq.getAllReadFiles()
	if err != nil {
		return
	}
	firstFile := true
	for _, file := range files {

		more := func(f string) bool {
			fh, err := os.Open(filepath.Join(dq.sdir, f))
			if err != nil {
				return false
			}
			if firstFile {
				fh.Seek(dq.pushReadOffset, os.SEEK_SET)
				firstFile = false
			}
			defer fh.Close()
			for {
				//Read Header
				rec, err := dq.readHeader(fh)
				if err != nil {
					return true
				}
				//Read Payload
				err = dq.readPayload(fh, rec)
				if err != nil {
					if err == io.EOF {
						return true
					}
					return false
				}

				paylo := rec.Payload
				le := len(paylo)

				data = append(data, paylo[:le-1])

				nroRecDone++
				if nroRecDone >= nroRec {
					return false
				}
			}
			return true
		}(file)
		if !more {
			return
		}
	}
	return
}


//POP =================================================================
func (dq *DiskQueue) pop() (data []byte, err error) {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()

	dq.pushReadFH.Seek(dq.pushReadOffset, os.SEEK_SET)
	//Read Header
	rec, err := dq.ReadHeader()
	if err == io.EOF && dq.pushReadFH.Name() != dq.pushFH.Name() {

		if errRotate := dq.rotatePop(); errRotate != nil {
			err = errRotate
			return
		}
		rec, err = dq.ReadHeader()
	}

	if err != nil {
		return
	}
	//Read Payload
	err = dq.ReadPayload(rec)
	if err != nil {
		return
	}

	paylo := rec.Payload
	le := len(paylo)
	data = append(data, paylo[:le-1]...)

	//Update offset
	offset := int64(HEADERSZ + len(paylo))
	dq.pushReadOffset += offset

	// Write POP record
	err = dq.WriteDelRecord(dq.pushReadOffset, rec.Crc)
	if err != nil {
		return
	}
	dq.popRecNro++

	dq.sleep = time.Now().UTC()
	return
}

func compressFile(fn string) error {
	err := exec.Command("gzip", fn).Run()
	if err != nil {
		return err
	}

	return nil
}

func (dq *DiskQueue) rotatePop() error {
	//Rotate
	//Close atual, move and open
	if err := dq.closeReader(); err != nil {
		return fmt.Errorf("rotatePop: Close %s [%s]", dq.sdir, err)
	}
	if dq.pushFH.Name() == dq.pushReadFH.Name() {
		err := dq.closeWriter()
		if err != nil {
			return err
		}
	}

	//Move
	filePushRead := dq.getFileNameToRead()
	sdirpopOrigin := filepath.Join(dq.sdir, "pop")
	sdirpushOrigin := filepath.Join(dq.sdir, filePushRead)
	sdirpopDestine := filepath.Join(dq.sdir, constBKP, "pop"+fmt.Sprintf("%s", time.Now().Format("20060102_150405.000")))
	sdirpushDestine := filepath.Join(dq.sdir, constBKP, fmt.Sprintf("%s%s", filePushRead, time.Now().Format("20060102_150405.000")))

	if err := os.Rename(sdirpopOrigin, sdirpopDestine); err != nil {
		return fmt.Errorf("rotatePop: move %s to %s [%s]", sdirpopOrigin, sdirpopDestine, err)
	}

	if err := os.Rename(sdirpushOrigin, sdirpushDestine); err != nil {
		return fmt.Errorf("rotatePop: move %s to %s [%s]", sdirpushOrigin, sdirpushDestine, err)
	}

	// Ignorar erros de compressão
	if err := compressFile(sdirpopDestine); err != nil {
		err = fmt.Errorf("rotatePop: compress %s [%s]", sdirpopDestine, err)
		fmt.Println(err.Error())
	}

	if err := compressFile(sdirpushDestine); err != nil {
		err = fmt.Errorf("rotatePop: compress %s [%s]", sdirpushDestine, err)
		fmt.Println(err.Error())
	}

	// verificar se ainda existe arquivo de push na pasta
	// testa se os arquivos de leitura e escrita se alcançaram
	if dq.pushFH.Name() == dq.pushReadFH.Name() {
		dq.pushFileSize = 0
		dq.currFilePush = 0
		dq.openToWrite()

	}

	if err := dq.openToRead(); err != nil {
		return fmt.Errorf("rotatePop: Open %s [%s]", dq.sdir, err)
	}

	//Zera Ponteiros
	dq.pushReadOffset = 0
	dq.pushRecNro = 0
	dq.pushReadNro = 0
	dq.popRecNro = 0

	return nil
}

func (dq *DiskQueue) WriteDelRecord(off int64, crcpush uint32) error {
	crc := int64CRC(off)
	buff := new(bytes.Buffer)
	buff.Write([]byte(fmt.Sprintf("%010d", crc)))
	buff.Write([]byte(" "))
	buff.Write([]byte(fmt.Sprintf("%020d", off)))
	buff.Write([]byte(" "))
	buff.Write([]byte(fmt.Sprintf("%010d", crcpush)))
	buff.Write([]byte("\n"))
	_, err := dq.popFH.Write(buff.Bytes())
	return err
}

//Read last del record
func (dq *DiskQueue) ReadDelRecord() (offset int64, err error) {
	//File is Empty
	curoffset, err := dq.popFH.Seek(0, 2)
	if curoffset == 0 {
		return
	}
	curoffset, err = dq.popFH.Seek(-DELRECSZ, 2)
	if err == io.EOF {
		err = nil
		return
	}
	offset, err = dq.offsetDelRecord()
	return
}

// Find offset of delRecord in current position
func (dq *DiskQueue) offsetDelRecord() (offset int64, err error) {
	var databuff []byte = make([]byte, DELRECSZ)
	datasz, err := dq.popFH.Read(databuff)
	if err != nil {
		return
	}
	if int32(datasz) != DELRECSZ {
		err = errors.New(fmt.Sprintf("Invalid DEL RECORD size. Expected %d got %d bytes", DELRECSZ, int32(datasz)))
		return
	}
	var crc uint32

	scrc := fmt.Sprintf("%s", databuff[0:10])
	//space 1 byte
	soff := fmt.Sprintf("%s", databuff[11:31])
	//newline 1 byte
	offset, _ = strconv.ParseInt(soff, 10, 64)
	nCrc, _ := strconv.ParseUint(scrc, 10, 0)
	crc = uint32(nCrc)

	checkcrc := int64CRC(offset)
	if checkcrc != crc {
		err = errors.New(fmt.Sprintf("Invalid last record CRC"))
		return
	}

	return
}

func (dq *DiskQueue) ReadHeader() (rec *Record, err error) {
	rec, err = dq.readHeader(dq.pushReadFH)
	return
}

func (dq *DiskQueue) readHeader(fh *os.File) (rec *Record, err error) {
	rec = &Record{0, 0, nil}
	var headbuff []byte = make([]byte, HEADERSZ)
	headsz, err := fh.Read(headbuff)
	if err != nil {
		return
	}

	if int32(headsz) != HEADERSZ {
		err = errors.New(fmt.Sprintf("Invalid Header size. Expected %d got %d bytes", HEADERSZ, int32(headsz)))
		return
	}

	slen := fmt.Sprintf("%s", headbuff[0:10])
	scrc := fmt.Sprintf("%s", headbuff[11:21])

	var nLen int
	nLen, err = strconv.Atoi(slen)
	if err != nil {
		err = fmt.Errorf("Invalid number data size: %s [%s] [%s]", fh.Name(), string(slen), err)
		return
	}
	var nCrc uint64
	nCrc, err = strconv.ParseUint(scrc, 10, 0)
	if err != nil {
		err = fmt.Errorf("Invalid number data crc: %s [%s] [%s]", fh.Name(), string(scrc), err)
		return
	}
	rec.Len = int32(nLen)
	rec.Crc = uint32(nCrc)

	return
}

func (dq *DiskQueue) ReadPayload(rec *Record) (err error) {
	err = dq.readPayload(dq.pushReadFH, rec)
	return
}

func (dq *DiskQueue) readPayload(fh *os.File, rec *Record) (err error) {
	var databuff []byte = make([]byte, rec.Len)
	var sz int
	sz, err = fh.Read(databuff)
	if err != nil {
		return
	}
	if int32(sz) != rec.Len {
		err = errors.New(fmt.Sprintf("Invalid Data size. Expected %d got %d bytes", rec.Len, sz))
		return
	}
	rec.Payload = databuff
	payloadCrc := sliceCRC(databuff)
	if rec.Crc != payloadCrc {
		err = errors.New("CRC not equal!")
	}
	return
}

func (dq *DiskQueue) SetCleanerTime(d time.Duration) {
	if !dq.clearStarted {
		dq.clearStarted = true
		go dq.cleanerProcess(d)
	}
}

func (dq *DiskQueue) cleanerProcess(d time.Duration) {
	pathbkp := filepath.Join(dq.sdir, constBKP)
	for {
		dirs, err := ioutil.ReadDir(pathbkp)
		if err != nil {
			// not found dirs
			time.Sleep(time.Hour)
			continue
		}
		for _, dir := range dirs {
			if !dir.IsDir() {
				if time.Since(dir.ModTime()) > d {
					os.Remove(filepath.Join(pathbkp, dir.Name()))
				}
			}
		}
		time.Sleep(time.Hour * 24)
	}
}

func (dq DiskQueue) getSizeFiles() (uint64, error) {
	t := uint64(0)
	files, err := ioutil.ReadDir(dq.sdir)
	if err != nil {
		return t, err
	}

	for _, file := range files {
		if skipFile(file) || !strings.HasPrefix(file.Name(), "push") {
			continue
		}
		t += uint64(file.Size())
	}

	return t, nil
}

//UTILS (copyfile)
func CopyFile(dst, src string) (int64, error) {
	sf, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer sf.Close()
	df, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer df.Close()
	return io.Copy(df, sf)
}

//DIRECTORY =================================================================
// A fileInfoList implements sort.Interface.
type fileInfoList []os.FileInfo

func (f fileInfoList) Len() int { return len(f) }
func (f fileInfoList) Less(i, j int) bool {
	a, _ := strconv.ParseUint(f[i].Name(), 10, 64)
	b, _ := strconv.ParseUint(f[j].Name(), 10, 64)
	return a < b
}
func (f fileInfoList) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

// ReadDir reads the directory named by dirname and returns
// a list of sorted directory entries.
// NUMERIC Order
func readDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	fi := make(fileInfoList, len(list))
	for i := range list {
		//fi[i] = &list[i]
		fi[i] = list[i]
	}
	sort.Sort(fi)
	return fi, nil
}

func lastFile(dir string) (fileNro uint64, err error) {
	var lastfile string

	//Verify directory
	listdir, err := readDir(dir)
	if err != nil {
		return
	}
	numFile := len(listdir)
	if numFile == 0 {
		lastfile = "1"
	} else {
		lenlist := numFile - 1
		lastfile = listdir[lenlist].Name()
	}

	fileNro, err = strconv.ParseUint(lastfile, 10, 64)
	return
}
