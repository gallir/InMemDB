package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/gallir/InMemDB"
)

type Thing struct {
	Txt string
	Val uint64
	Age uint64
}

func (t Thing) Less(u inmemdb.DBItem) bool {
	return t.Val < u.(*Thing).Val
}

func (t Thing) Key() string {
	return t.Txt
}

func main() {
	// Open the data.db file. It will be created if it doesn't exist.
	start := time.Now()
	db, err := inmemdb.Open("data.db")
	if err != nil {
		log.Fatal(err)
	}

	db.Update(create_index)
	db.Update(create_elements)

	db.View(func(tx *inmemdb.Tx) error {
		/*
			tx.Descend("ages", func(value inmemdb.DBItem) bool {
				t := value.(*Thing)
				fmt.Printf("Name: %s Age: %d Val: %d\n", t.Txt, t.Age, t.Val)
				return true
			})
		*/
		r, e := tx.Get("Thing 6")
		fmt.Println("got", r, e)

		return nil
	})

	defer db.Close()
	fmt.Println("Elapsed", time.Since(start))
}

func create_index(tx *inmemdb.Tx) error {
	log.Println("hola")
	tx.CreateIndex("vals", []string{"Val"}, inmemdb.IndexUint64)
	return tx.CreateIndex("ages", []string{"Age"}, inmemdb.IndexUint64)
}

func create_elements(tx *inmemdb.Tx) error {
	for i := 0; i < 100; i++ {
		t := &Thing{
			Txt: fmt.Sprintf("Thing %d", i),
			Val: rand.Uint64() % 10000, // uint64(i),
			Age: rand.Uint64() % 99,
		}
		tx.Set(t, nil)
	}
	return nil
}
