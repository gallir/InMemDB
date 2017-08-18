package inmemdb

import (
	"fmt"

	"github.com/cznic/b"
)

type BTree struct {
	tree *b.Tree
	ctx  interface{}
	cmp  func(a, b, ctx interface{}) int
	name string
}

func New(name string, cmp func(a, b, ctx interface{}) int, ctx interface{}) (t *BTree) {
	fmt.Println("New", cmp, ctx)
	t = &BTree{
		name: name,
		cmp:  cmp,
		ctx:  ctx,
		tree: b.TreeNew(
			func(a, b interface{}) int {
				return t.Less(a, b)
			}),
	}
	return
}

func (t *BTree) Less(a, b interface{}) int {
	return t.cmp(a, b, t.ctx)
}

func (t *BTree) Get(k interface{}) (r interface{}) {
	r, ok := t.tree.Get(k)
	if !ok {
		return nil
	}
	return
}

func (t *BTree) Delete(k interface{}) (r interface{}) {
	r, ok := t.tree.Get(k)
	if !ok {
		return nil
	}

	t.tree.Delete(k)
	return
}

func (t *BTree) Insert(k *dbItem) {
	k.insert = true
	t.tree.Set(k, k)
	k.insert = false
}

func (t *BTree) ReplaceOrInsert(k *dbItem) interface{} {
	// Less will check up only the main key
	k.insert = true

	old, _ := t.tree.Put(k,
		func(old interface{}, exists bool) (interface{}, bool) {
			if exists {
				var o string
				if old != nil {
					o = old.(*dbItem).key
				}
				fmt.Println("Put duplicated", t.name, t.tree.Len(), k.key, o)
			}
			return k, true
		})

	k.insert = false
	return old
}

func (t *BTree) Ascend(it func(item interface{}) bool) {
	t.AscendRange(nil, nil, it)
}

func (t *BTree) AscendRange(start, stop *dbItem, it func(item interface{}) bool) {
	var en *b.Enumerator
	var ok bool
	var err error

	if start != nil {
		en, ok = t.tree.Seek(start)
		fmt.Println("Asc start", start, t.name, ok, start.insert, start.key)
	} else {
		en, err = t.tree.SeekFirst()
	}
	if !ok || err != nil {
		return
	}
	defer en.Close()

	i := 0
	for {
		var k, v interface{}
		var err error
		if k, v, err = en.Next(); err != nil {
			fmt.Println("No Next SENT", k, v, err)
			return
		}

		if stop != nil {
			if t.cmp(v, stop, t.ctx) > 0 {
				fmt.Println("STOP", i)
				return
			}
		}

		i++

		if !it(v) {
			fmt.Println("SENT", i)
			return
		}
	}
}

func (t *BTree) Len() int {
	return t.Len()
}
