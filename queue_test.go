package diskqueue

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
)

const (
	TmpDir      = "/tmp/content_queue/"
	DataContent = "%010d"
	Amount      = 1000
	Size        = 1024 * 10
)

var queue *DiskQueue

func init() {
	os.RemoveAll(TmpDir)
}

func create(t *testing.T) {
	var err error
	queue, err = New(TmpDir)
	if err != nil {
		t.Fatalf("error %v", err)
	}
	queue.MaxSize(Size)
	return
}

func TestCreate(t *testing.T) {
	create(t)
}

func getContent(i int) []byte {
	return []byte(fmt.Sprintf(DataContent, i))
}

func TestDiskQueue_Push(t *testing.T) {
	err := queue.Push(getContent(999999))
	if err != nil {
		t.Fatalf("error push %v", err)
	}
}

func TestPop(t *testing.T) {
	data, err := queue.Pop()
	if err != nil {
		if err != io.EOF {
			t.Fatalf("error pop %v", err)
		}
		return
	}
	if !bytes.Equal(data, getContent(999999)) {
		t.Fatalf("not equal, expected %v, got %v", string(getContent(999999)), string(data))
	}
}

func TestBulkPush(t *testing.T) {
	for i := 0; i < Amount; i++ {
		// t.Logf("Inserindo registro: %d", i)
		err := queue.Push([]byte(getContent(i)))
		if err != nil {
			if err != io.EOF {
				t.Fatalf("error pop %v", err)
			}
			return
		}
	}
}

func TestBulkPop(t *testing.T) {

	for i := 0; ; i++ {
		// t.Logf("Lendo registro: %d", i)
		data, err := queue.Pop()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("error pop %v", err)
			}
			return
		}
		if !bytes.Equal(data, getContent(i)) {
			t.Fatalf("error pop, expected %s, got %s", string(getContent(i)), string(data))
		}
	}
}

func TestIntercal(t *testing.T) {
	for i := 0; i <= Amount*2; i++ {
		err := queue.Push(getContent(i))
		if err != nil {
			t.Fatalf("%s error Push %s", "TestIntercal", err)
		}
		data, err := queue.Pop()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("error pop %v", err)
			}
			if err == io.EOF {
				return
			}
		}
		if !bytes.Equal(data, getContent(i)) {
			t.Fatalf("error pop, expected %s, got %s", string(getContent(i)), string(data))
		}
	}
}

func TestInsert(t *testing.T) {
	for i := 0; i < Amount*10; i++ {
		err := queue.Push(getContent(i))
		if err != nil {
			t.Fatalf("%s error Push %s", "TestIntercal", err)
		}
	}
}

func TestDiskQueue_Close(t *testing.T) {
	err := queue.Close()
	if err != nil {
		t.Fatalf("close, expected nil, got %v", err)
	}
}

func TestDiskQueue_New(t *testing.T) {
	create(t)
}

func TestDiskQueue_PopReadOnly(t *testing.T) {
	records, nrec := queue.PopReadOnly(Amount * 10)
	if nrec != Amount*10 {
		t.Fatalf("error, expected %d, got %d", Amount*10, nrec)
	}
	for i, data := range records {
		if !bytes.Equal(data, getContent(i)) {
			t.Fatalf("error pop, expected %s, got %s", string(getContent(i)), string(data))
		}
	}
}

func TestBulkPopAfterClose(t *testing.T) {

	for i := 0; ; i++ {
		// t.Logf("Lendo registro: %d", i)
		data, err := queue.Pop()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("error pop %v", err)
			}
			return
		}
		if !bytes.Equal(data, getContent(i)) {
			t.Fatalf("error pop, expected %s, got %s", string(getContent(i)), string(data))
		}
	}
}

func TestConcurrencePush(t *testing.T) {
	var num = 10
	var wg sync.WaitGroup
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func() {
			for j := 0; j < Amount; j++ {
				err := queue.Push(getContent(j))
				if err != nil {
					t.Fatalf("%s error Push %s", "TestConcurrence", err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestConcurrencePop(t *testing.T) {
	var num = 10
	var wg sync.WaitGroup
	wg.Add(num)
	var mutex sync.Mutex
	var records int
	for j := 0; j < num; j++ {
		go func() {
			defer wg.Done()
			for {
				// t.Logf("Lendo registro")
				_, err := queue.Pop()
				if err != nil {
					if err != io.EOF {
						t.Fatalf("error pop %v", err)
					}
					// t.Logf("%v",err)
					break
				}
				mutex.Lock()
				records++
				mutex.Unlock()
			}

		}()
	}
	wg.Wait()
	if records != Amount*10 {
		t.Fatalf("expected %d, got %d", Amount, records)
	}
}

func TestRemoveAll(t *testing.T) {
	os.RemoveAll(TmpDir)
}
