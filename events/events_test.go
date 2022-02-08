package events

// // Sequential write
// func TestSequentialWrite(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())

// 	e := EventEmitter{}
// 	expectedClients := 10
// 	expectedEvents := 10

// 	chs := make([]<-chan Event, expectedClients)

// 	for i := 0; i < expectedClients; i++ {
// 		chs[i] = e.Subscribe(ctx)
// 	}

// 	go func() {
// 		for i := 0; i < expectedEvents; i++ {
// 			e.Emit(ctx, fmt.Sprintf("%d", i))
// 		}
// 	}()

// 	for i := 0; i < expectedClients; i++ {
// 		for j := 0; j < expectedEvents; j++ {
// 			item := <-chs[i]
// 			if fmt.Sprintf("%s", item) != fmt.Sprintf("%d", j) {
// 				t.Fatalf("%s should be equal to %d", item, j)
// 			}
// 		}
// 	}

// 	cancel()

// 	<-time.After(time.Millisecond * 100)

// 	for i := 0; i < expectedClients; i++ {
// 		for range chs[i] {
// 			t.Fatal("should not occur")
// 		}
// 	}
// }

// func TestMissingListeners(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	e := EventEmitter{}
// 	expectedEvents := 10

// 	go func() {
// 		for i := 0; i < expectedEvents; i++ {
// 			e.Emit(ctx, fmt.Sprintf("%d", i))
// 		}

// 		<-time.After(10 * time.Millisecond)
// 	}()

// 	<-time.After(100 * time.Millisecond)
// }

// func TestPartialListeners(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	e := EventEmitter{}

// 	go func() {
// 		for i := 0; i < 5; i++ {
// 			e.Emit(ctx, fmt.Sprintf("%d", i))
// 		}
// 	}()

// 	<-time.After(time.Millisecond * 100)

// 	subCtx, subCancel := context.WithCancel(context.Background())
// 	sub := e.Subscribe(subCtx)

// 	<-time.After(time.Millisecond * 100)

// 	for i := 5; i < 10; i++ {
// 		e.Emit(ctx, fmt.Sprintf("%d", i))
// 	}

// 	<-time.After(time.Millisecond * 100)

// 	for i := 5; i < 10; i++ {
// 		item := <-sub
// 		itemStr, ok := item.(string)
// 		if !ok {
// 			t.Fatalf("unable to cast")
// 		}

// 		if itemStr != fmt.Sprintf("%d", i) {
// 			t.Errorf("(%s) should be equal to (%d)", itemStr, i)
// 		}
// 	}

// 	<-time.After(time.Second)

// 	subCancel()

// 	for range sub {
// 		t.Fatalf("this should not happen")
// 	}
// }
