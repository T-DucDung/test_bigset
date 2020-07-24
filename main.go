package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenStars/EtcdBackendService/StringBigsetService"
	"github.com/OpenStars/EtcdBackendService/StringBigsetService/bigset/thrift/gen-go/openstars/core/bigset/generic"
)

type User struct {
	UserID   string `json:"user_id" xml:"user_id"`
	Account  string `json:"account" xml:"account"`
	Password string `json:"password" xml:"password"`
	UnitID   string `json:"unit_id" xml:"unit_id"`
}

func (this *User) String() string {
	return this.UserID
}

var (
	bigsetIf StringBigsetService.StringBigsetServiceIf
	count    int32
)

func MarshalBytes(object interface{}) ([]byte, []byte, error) {
	var key []byte
	if object == nil {
		return nil, nil, errors.New("object must be not nil")
	}

	obj, err := json.Marshal(object)
	if err != nil {
		return nil, nil, err
	}

	key = []byte(fmt.Sprintf("%v", object))
	test := fmt.Sprintf("%v", object)
	log.Println(test, "-- test")

	return obj, key, nil
}

func UnMarshalArrayTItem(objects []*generic.TItem) ([]User, error) {
	objs := make([]User, 0)

	for _, object := range objects {
		obj := User{}
		err := json.Unmarshal(object.GetValue(), &obj)

		if err != nil {
			return make([]User, 0), err
		}

		objs = append(objs, obj)
	}

	return objs, nil
}

func CreateUser(id <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range id {
		var users User
		users.UserID = j
		users.UnitID = "string"
		users.Account = "string"
		users.Password = "string"
		bUser, key, err := MarshalBytes(users)
		if err != nil {
			log.Println(err)
		}
		bigsetIf.BsPutItem("User", &generic.TItem{
			Key:   key,
			Value: bUser,
		})
	}
}

func ClearAll(temp <-chan int) {
	for n := range temp {
		slide, _ := bigsetIf.BsGetSliceR("User", int32(n), atomic.AddInt32(&count, int32(n)))
		log.Println(count)
		for _, u := range slide {
			err := bigsetIf.BsRemoveItem(generic.TStringKey(fmt.Sprintf("User")), u.GetKey())
			log.Println(err)
		}
	}
}

func main() {
	var str = []string{"127.0.0.1:2379"}

	bigsetIf = StringBigsetService.NewStringBigsetServiceModel2(str, "ducdung", "127.0.0.1", "2739")

	count1, _ := bigsetIf.GetTotalCount(generic.TStringKey(fmt.Sprintf("User")))
	log.Println(count1)

	// slide, _ := bigsetIf.BsGetSliceR("User", 0, int32(count1))
	// for _, u := range slide {
	// 	bigsetIf.BsRemoveItem(generic.TStringKey(fmt.Sprintf("User")), u.GetKey())
	// }

	// count = 0
	// num := 10
	// temp := make(chan int, num)
	// for w := 1; w <= num; w++ {
	// 	go ClearAll(temp)
	// }
	// wg := new(sync.WaitGroup)
	// start := time.Now()
	// for i := 0; i < int(count1); i = i + 1000 {
	// 	temp <- int(i)
	// }
	// wg.Wait()
	// elapsed := time.Since(start)
	// close(temp)
	// log.Println(elapsed)slide, _ := bigsetIf.BsGetSliceR("User", 0, int32(count1))
	// for _, u := range slide {
	// 	bigsetIf.BsRemoveItem(generic.TStringKey(fmt.Sprintf("User")), u.GetKey())
	// }

	var wg sync.WaitGroup
	num := 10
	id := make(chan string, num)
	for w := 1; w <= num; w++ {
		wg.Add(1)
		go CreateUser(id, &wg)
	}
	start := time.Now()
	for i := 51; i <= 100; i++ {
		id <- strconv.Itoa(i)
	}
	close(id)
	wg.Wait()
	elapsed := time.Since(start)
	log.Println(elapsed)

	count1, _ = bigsetIf.GetTotalCount(generic.TStringKey(fmt.Sprintf("User")))

	log.Println(count1)
}
