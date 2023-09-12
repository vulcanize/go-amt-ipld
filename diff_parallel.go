package amt

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func ParallelDiff(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, workers int64, opts ...Option) ([]*Change, error) {
	prevAmt, err := LoadAMT(ctx, prevBs, prev, opts...)
	if err != nil {
		return nil, xerrors.Errorf("loading previous root: %w", err)
	}

	prevCtx := &nodeContext{
		bs:       prevBs,
		bitWidth: prevAmt.bitWidth,
		height:   prevAmt.height,
	}

	curAmt, err := LoadAMT(ctx, curBs, cur, opts...)
	if err != nil {
		return nil, xerrors.Errorf("loading current root: %w", err)
	}

	// TODO: remove when https://github.com/filecoin-project/go-amt-ipld/issues/54 is closed.
	if curAmt.bitWidth != prevAmt.bitWidth {
		return nil, xerrors.Errorf("diffing AMTs with differing bitWidths not supported (prev=%d, cur=%d)", prevAmt.bitWidth, curAmt.bitWidth)
	}

	curCtx := &nodeContext{
		bs:       curBs,
		bitWidth: curAmt.bitWidth,
		height:   curAmt.height,
	}

	// edge case of diffing an empty AMT against non-empty
	if prevAmt.count == 0 && curAmt.count != 0 {
		return addAll(ctx, curCtx, curAmt.node, 0)
	}
	if prevAmt.count != 0 && curAmt.count == 0 {
		return removeAll(ctx, prevCtx, prevAmt.node, 0)
	}
	out := make(chan *Change)
	differ, ctx := newDiffScheduler(ctx, workers, &task{
		prevCtx: prevCtx,
		curCtx:  curCtx,
		prev:    prevAmt.node,
		cur:     curAmt.node,
		offset:  0,
	})
	differ.startScheduler(ctx)
	differ.startWorkers(ctx, out)

	var changes []*Change
	done := make(chan struct{})
	go func() {
		for change := range out {
			changes = append(changes, change)
		}
		close(done)
	}()

	err = differ.grp.Wait()
	close(out)
	<-done

	return changes, err
}

func ParallelDiffTrackedWithNodeSink(ctx context.Context, prevBs, curBs cbor.IpldStore, prev, cur cid.Cid, workers int64, sink cbg.CBORUnmarshaler, opts ...Option) ([]*TrackedChange, error) {
	prevAmt, err := LoadAMT(ctx, prevBs, prev, opts...)
	if err != nil {
		return nil, xerrors.Errorf("loading previous root: %w", err)
	}

	prevCtx := &nodeContext{
		bs:       prevBs,
		bitWidth: prevAmt.bitWidth,
		height:   prevAmt.height,
	}

	curAmt, err := LoadAMT(ctx, curBs, cur, opts...)
	if err != nil {
		return nil, xerrors.Errorf("loading current root: %w", err)
	}

	// TODO: remove when https://github.com/filecoin-project/go-amt-ipld/issues/54 is closed.
	if curAmt.bitWidth != prevAmt.bitWidth {
		return nil, xerrors.Errorf("diffing AMTs with differing bitWidths not supported (prev=%d, cur=%d)", prevAmt.bitWidth, curAmt.bitWidth)
	}

	curCtx := &nodeContext{
		bs:       curBs,
		bitWidth: curAmt.bitWidth,
		height:   curAmt.height,
	}

	// edge case of diffing an empty AMT against non-empty
	if prevAmt.count == 0 && curAmt.count != 0 {
		return addAllTrackWithNodeSink(ctx, curCtx, curAmt.node, 0, nil, sink, []int{})
	}
	if prevAmt.count != 0 && curAmt.count == 0 {
		return removeAllTracked(ctx, prevCtx, prevAmt.node, 0, []int{})
	}
	out := make(chan *TrackedChange)
	differ, ctx := newTrackedScheduler(ctx, workers, &trackedTask{
		task: task{
			prevCtx: prevCtx,
			curCtx:  curCtx,
			prev:    prevAmt.node,
			cur:     curAmt.node,
			offset:  0,
		},
		path: []int{},
	})
	differ.startScheduler(ctx)
	differ.startWorkers(ctx, out)

	var changes []*TrackedChange
	done := make(chan struct{})
	go func() {
		for change := range out {
			changes = append(changes, change)
		}
		close(done)
	}()

	err = differ.grp.Wait()
	close(out)
	<-done

	return changes, err
}

func parallelAddAll(ctx context.Context, nc *nodeContext, node *node, offset uint64, out chan *Change) error {
	return node.forEachAt(ctx, nc.bs, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
		out <- &Change{
			Type:   Add,
			Key:    index,
			Before: nil,
			After:  deferred,
		}
		return nil
	})
}

func parallelAddAllTrackedWithNodeSink(ctx context.Context, nc *nodeContext, node *node, offset uint64, initialPath []int, sink cbg.CBORUnmarshaler, out chan *TrackedChange) error {
	return node.forEachAtParallelTrackedWithNodeSink(ctx, nc.bs, initialPath, nc.bitWidth, nc.height, 0, offset, sink, func(index uint64, deferred *cbg.Deferred, path []int) error {
		out <- &TrackedChange{
			Change: Change{
				Type:   Add,
				Key:    index,
				Before: nil,
				After:  deferred,
			},
			Path: path,
		}
		return nil
	}, 16)
}

func parallelRemoveAll(ctx context.Context, nc *nodeContext, node *node, offset uint64, out chan *Change) error {
	return node.forEachAt(ctx, nc.bs, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred) error {
		out <- &Change{
			Type:   Remove,
			Key:    index,
			Before: deferred,
			After:  nil,
		}
		return nil
	})
}

func parallelRemoveAllTracked(ctx context.Context, nc *nodeContext, node *node, offset uint64, initialPath []int, out chan *TrackedChange) error {
	return node.forEachAtParallelTracked(ctx, nc.bs, initialPath, nc.bitWidth, nc.height, 0, offset, func(index uint64, deferred *cbg.Deferred, path []int) error {
		out <- &TrackedChange{
			Change: Change{
				Type:   Remove,
				Key:    index,
				Before: deferred,
				After:  nil,
			},
			Path: path,
		}
		return nil
	}, 16)
}

func parallelDiffLeaves(prev, cur *node, offset uint64, out chan *Change) error {
	if len(prev.values) != len(cur.values) {
		return fmt.Errorf("node leaves have different numbers of values (prev=%d, cur=%d)", len(prev.values), len(cur.values))
	}

	for i, prevVal := range prev.values {
		index := offset + uint64(i)

		curVal := cur.values[i]
		if prevVal == nil && curVal == nil {
			continue
		}

		if prevVal == nil && curVal != nil {
			out <- &Change{
				Type:   Add,
				Key:    index,
				Before: nil,
				After:  curVal,
			}
			continue
		}

		if prevVal != nil && curVal == nil {
			out <- &Change{
				Type:   Remove,
				Key:    index,
				Before: prevVal,
				After:  nil,
			}
			continue
		}

		if !bytes.Equal(prevVal.Raw, curVal.Raw) {
			out <- &Change{
				Type:   Modify,
				Key:    index,
				Before: prevVal,
				After:  curVal,
			}
		}

	}
	return nil
}

func parallelDiffLeavesTrackedWithNodeSink(ctx context.Context, bitWidth uint, prev, cur *node, offset uint64, path []int, sink cbg.CBORUnmarshaler, out chan *TrackedChange) error {
	if len(prev.values) != len(cur.values) {
		return fmt.Errorf("node leaves have different numbers of values (prev=%d, cur=%d)", len(prev.values), len(cur.values))
	}

	if sink != nil {
		b := bytes.NewBuffer(nil)
		internalNode, err := cur.compact(ctx, bitWidth, 0)
		if err != nil {
			return err
		}
		if err := internalNode.MarshalCBOR(b); err != nil {
			return err
		}
		if err := sink.UnmarshalCBOR(b); err != nil {
			return err
		}
	}

	l := len(path)
	for i, prevVal := range prev.values {
		subPath := make([]int, l, l+1)
		copy(subPath, path)
		subPath = append(subPath, i)
		index := offset + uint64(i)

		curVal := cur.values[i]
		if prevVal == nil && curVal == nil {
			continue
		}

		if prevVal == nil && curVal != nil {
			out <- &TrackedChange{
				Change: Change{
					Type:   Add,
					Key:    index,
					Before: nil,
					After:  curVal,
				},
				Path: subPath,
			}
			continue
		}

		if prevVal != nil && curVal == nil {
			out <- &TrackedChange{
				Change: Change{
					Type:   Remove,
					Key:    index,
					Before: prevVal,
					After:  nil,
				},
				Path: subPath,
			}
			continue
		}

		if !bytes.Equal(prevVal.Raw, curVal.Raw) {
			out <- &TrackedChange{
				Change: Change{
					Type:   Modify,
					Key:    index,
					Before: prevVal,
					After:  curVal,
				},
				Path: subPath,
			}
		}

	}
	return nil
}

type task struct {
	prevCtx, curCtx *nodeContext
	prev, cur       *node
	offset          uint64
}

type trackedTask struct {
	task
	path []int
}

func newDiffScheduler(ctx context.Context, numWorkers int64, rootTasks ...*task) (*diffScheduler, context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	s := &diffScheduler{
		numWorkers: numWorkers,
		stack:      rootTasks,
		in:         make(chan *task, numWorkers),
		out:        make(chan *task, numWorkers),
		grp:        grp,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

func newTrackedScheduler(ctx context.Context, numWorkers int64, rootTasks ...*trackedTask) (*trackedScheduler, context.Context) {
	grp, ctx := errgroup.WithContext(ctx)
	s := &trackedScheduler{
		numWorkers: numWorkers,
		stack:      rootTasks,
		in:         make(chan *trackedTask, numWorkers),
		out:        make(chan *trackedTask, numWorkers),
		grp:        grp,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

type diffScheduler struct {
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*task
	// inbound and outbound tasks
	in, out chan *task
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
}

type trackedScheduler struct {
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*trackedTask
	// inbound and outbound tasks
	in, out chan *trackedTask
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
	// node sink
	sink cbg.CBORUnmarshaler
}

func (s *diffScheduler) enqueueTask(task *task) {
	s.taskWg.Add(1)
	s.in <- task
}

func (t *trackedScheduler) enqueueTask(task *trackedTask) {
	t.taskWg.Add(1)
	t.in <- task
}

func (s *diffScheduler) startScheduler(ctx context.Context) {
	s.grp.Go(func() error {
		defer func() {
			close(s.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range s.out {
				s.taskWg.Done()
			}
			// Because the workers may have enqueued additional tasks.
			for range s.in {
				s.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			s.taskWg.Wait()
			close(s.in)
		}()
		for {
			if n := len(s.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				case s.out <- s.stack[n]:
					s.stack[n] = nil
					s.stack = s.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				}
			}
		}
	})
}

func (t *trackedScheduler) startScheduler(ctx context.Context) {
	t.grp.Go(func() error {
		defer func() {
			close(t.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range t.out {
				t.taskWg.Done()
			}
			// Because the workers may have enqueued additional tasks.
			for range t.in {
				t.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			t.taskWg.Wait()
			close(t.in)
		}()
		for {
			if n := len(t.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-t.in:
					if !ok {
						return nil
					}
					t.stack = append(t.stack, newJob)
				case t.out <- t.stack[n]:
					t.stack[n] = nil
					t.stack = t.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-t.in:
					if !ok {
						return nil
					}
					t.stack = append(t.stack, newJob)
				}
			}
		}
	})
}

func (s *diffScheduler) startWorkers(ctx context.Context, out chan *Change) {
	for i := int64(0); i < s.numWorkers; i++ {
		s.grp.Go(func() error {
			for task := range s.out {
				if err := s.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (t *trackedScheduler) startWorkers(ctx context.Context, out chan *TrackedChange) {
	for i := int64(0); i < t.numWorkers; i++ {
		t.grp.Go(func() error {
			for task := range t.out {
				if err := t.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (s *diffScheduler) work(ctx context.Context, todo *task, results chan *Change) error {
	defer s.taskWg.Done()

	prev := todo.prev
	prevCtx := todo.prevCtx
	cur := todo.cur
	curCtx := todo.curCtx
	offset := todo.offset

	if prev == nil && cur == nil {
		return nil
	}

	if prev == nil {
		return parallelAddAll(ctx, curCtx, cur, offset, results)
	}

	if cur == nil {
		return parallelRemoveAll(ctx, prevCtx, prev, offset, results)
	}

	if prevCtx.height == 0 && curCtx.height == 0 {
		return parallelDiffLeaves(prev, cur, offset, results)
	}

	if curCtx.height > prevCtx.height {
		subCount := curCtx.nodesAtHeight()
		for i, ln := range cur.links {
			if ln == nil || ln.cid == cid.Undef {
				continue
			}

			subCtx := &nodeContext{
				bs:       curCtx.bs,
				bitWidth: curCtx.bitWidth,
				height:   curCtx.height - 1,
			}

			subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			if i == 0 {
				s.enqueueTask(&task{
					prevCtx: prevCtx,
					curCtx:  subCtx,
					prev:    prev,
					cur:     subn,
					offset:  offs,
				})
			} else {
				err := parallelAddAll(ctx, subCtx, subn, offs, results)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	if prevCtx.height > curCtx.height {
		subCount := prevCtx.nodesAtHeight()
		for i, ln := range prev.links {
			if ln == nil || ln.cid == cid.Undef {
				continue
			}

			subCtx := &nodeContext{
				bs:       prevCtx.bs,
				bitWidth: prevCtx.bitWidth,
				height:   prevCtx.height - 1,
			}

			subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)

			if i == 0 {
				s.enqueueTask(&task{
					prevCtx: subCtx,
					curCtx:  curCtx,
					prev:    subn,
					cur:     cur,
					offset:  offs,
				})
			} else {
				err := parallelRemoveAll(ctx, subCtx, subn, offs, results)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	// sanity check
	if prevCtx.height != curCtx.height {
		return fmt.Errorf("comparing non-leaf nodes of unequal heights (%d, %d)", prevCtx.height, curCtx.height)
	}

	if len(prev.links) != len(cur.links) {
		return fmt.Errorf("nodes have different numbers of links (prev=%d, cur=%d)", len(prev.links), len(cur.links))
	}

	if prev.links == nil || cur.links == nil {
		return fmt.Errorf("nodes have no links")
	}

	subCount := prevCtx.nodesAtHeight()
	for i := range prev.links {
		// Neither previous or current links are in use
		if prev.links[i] == nil && cur.links[i] == nil {
			continue
		}

		// Previous had link, current did not
		if prev.links[i] != nil && cur.links[i] == nil {
			if prev.links[i].cid == cid.Undef {
				continue
			}

			subCtx := &nodeContext{
				bs:       prevCtx.bs,
				bitWidth: prevCtx.bitWidth,
				height:   prevCtx.height - 1,
			}

			subn, err := prev.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			err = parallelRemoveAll(ctx, subCtx, subn, offs, results)
			if err != nil {
				return err
			}
			continue
		}

		// Current has link, previous did not
		if prev.links[i] == nil && cur.links[i] != nil {
			if cur.links[i].cid == cid.Undef {
				continue
			}

			subCtx := &nodeContext{
				bs:       curCtx.bs,
				bitWidth: curCtx.bitWidth,
				height:   curCtx.height - 1,
			}

			subn, err := cur.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			err = parallelAddAll(ctx, subCtx, subn, offs, results)
			if err != nil {
				return err
			}
			continue
		}

		// Both previous and current have links to diff
		if prev.links[i].cid == cur.links[i].cid {
			continue
		}

		prevSubCtx := &nodeContext{
			bs:       prevCtx.bs,
			bitWidth: prevCtx.bitWidth,
			height:   prevCtx.height - 1,
		}

		prevSubn, err := prev.links[i].load(ctx, prevSubCtx.bs, prevSubCtx.bitWidth, prevSubCtx.height)
		if err != nil {
			return err
		}

		curSubCtx := &nodeContext{
			bs:       curCtx.bs,
			bitWidth: curCtx.bitWidth,
			height:   curCtx.height - 1,
		}

		curSubn, err := cur.links[i].load(ctx, curSubCtx.bs, curSubCtx.bitWidth, curSubCtx.height)
		if err != nil {
			return err
		}

		offs := offset + (uint64(i) * subCount)

		s.enqueueTask(&task{
			prevCtx: prevSubCtx,
			curCtx:  curSubCtx,
			prev:    prevSubn,
			cur:     curSubn,
			offset:  offs,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *trackedScheduler) work(ctx context.Context, todo *trackedTask, results chan *TrackedChange) error {
	defer t.taskWg.Done()

	prev := todo.prev
	prevCtx := todo.prevCtx
	cur := todo.cur
	curCtx := todo.curCtx
	offset := todo.offset

	if prev == nil && cur == nil {
		return nil
	}

	if prev == nil {
		return parallelAddAllTrackedWithNodeSink(ctx, curCtx, cur, offset, todo.path, t.sink, results)
	}

	if cur == nil {
		return parallelRemoveAllTracked(ctx, prevCtx, prev, offset, todo.path, results)
	}

	if prevCtx.height == 0 && curCtx.height == 0 {
		return parallelDiffLeavesTrackedWithNodeSink(ctx, curCtx.bitWidth, prev, cur, offset, todo.path, t.sink, results)
	}

	l := len(todo.path)
	if curCtx.height > prevCtx.height {
		if t.sink != nil {
			b := bytes.NewBuffer(nil)
			internalNode, err := cur.compact(ctx, curCtx.bitWidth, 0)
			if err != nil {
				return err
			}
			if err := internalNode.MarshalCBOR(b); err != nil {
				return err
			}
			if err := t.sink.UnmarshalCBOR(b); err != nil {
				return err
			}
		}

		subCount := curCtx.nodesAtHeight()
		for i, ln := range cur.links {
			if ln == nil || ln.cid == cid.Undef {
				continue
			}

			subPath := make([]int, l, l+1)
			copy(subPath, todo.path)
			subPath = append(subPath, i)

			subCtx := &nodeContext{
				bs:       curCtx.bs,
				bitWidth: curCtx.bitWidth,
				height:   curCtx.height - 1,
			}

			subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			if i == 0 {
				t.enqueueTask(&trackedTask{
					task: task{
						prevCtx: prevCtx,
						curCtx:  subCtx,
						prev:    prev,
						cur:     subn,
						offset:  offs,
					},
					path: subPath,
				})
			} else {
				err := parallelAddAllTrackedWithNodeSink(ctx, subCtx, subn, offs, subPath, t.sink, results)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	if prevCtx.height > curCtx.height {
		subCount := prevCtx.nodesAtHeight()
		sunk := false
		for i, ln := range prev.links {
			if ln == nil || ln.cid == cid.Undef {
				continue
			}

			subPath := make([]int, l, l+1)
			copy(subPath, todo.path)
			subPath = append(subPath, i)

			subCtx := &nodeContext{
				bs:       prevCtx.bs,
				bitWidth: prevCtx.bitWidth,
				height:   prevCtx.height - 1,
			}

			subn, err := ln.load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)

			if i == 0 {
				sunk = true
				t.enqueueTask(&trackedTask{
					task: task{
						prevCtx: subCtx,
						curCtx:  curCtx,
						prev:    subn,
						cur:     cur,
						offset:  offs,
					},
					path: todo.path,
				})
			} else {
				err := parallelRemoveAllTracked(ctx, subCtx, subn, offs, subPath, results)
				if err != nil {
					return err
				}
			}
		}
		if !sunk && t.sink != nil {
			b := bytes.NewBuffer(nil)
			internalNode, err := cur.compact(ctx, curCtx.bitWidth, 0)
			if err != nil {
				return err
			}
			if err := internalNode.MarshalCBOR(b); err != nil {
				return err
			}
			if err := t.sink.UnmarshalCBOR(b); err != nil {
				return err
			}
		}

		return nil
	}

	// sanity check
	if prevCtx.height != curCtx.height {
		return fmt.Errorf("comparing non-leaf nodes of unequal heights (%d, %d)", prevCtx.height, curCtx.height)
	}

	if len(prev.links) != len(cur.links) {
		return fmt.Errorf("nodes have different numbers of links (prev=%d, cur=%d)", len(prev.links), len(cur.links))
	}

	if prev.links == nil || cur.links == nil {
		return fmt.Errorf("nodes have no links")
	}

	if t.sink != nil {
		b := bytes.NewBuffer(nil)
		b.Reset()
		internalNode, err := cur.compact(ctx, curCtx.bitWidth, 0)
		if err != nil {
			return err
		}
		if err := internalNode.MarshalCBOR(b); err != nil {
			return err
		}
		if err := t.sink.UnmarshalCBOR(b); err != nil {
			return err
		}
	}

	subCount := prevCtx.nodesAtHeight()
	for i := range prev.links {
		// Neither previous or current links are in use
		if prev.links[i] == nil && cur.links[i] == nil {
			continue
		}

		subPath := make([]int, l, l+1)
		copy(subPath, todo.path)
		subPath = append(subPath, i)

		// Previous had link, current did not
		if prev.links[i] != nil && cur.links[i] == nil {
			if prev.links[i].cid == cid.Undef {
				continue
			}

			subCtx := &nodeContext{
				bs:       prevCtx.bs,
				bitWidth: prevCtx.bitWidth,
				height:   prevCtx.height - 1,
			}

			subn, err := prev.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			err = parallelRemoveAllTracked(ctx, subCtx, subn, offs, subPath, results)
			if err != nil {
				return err
			}
			continue
		}

		// Current has link, previous did not
		if prev.links[i] == nil && cur.links[i] != nil {
			if cur.links[i].cid == cid.Undef {
				continue
			}

			subCtx := &nodeContext{
				bs:       curCtx.bs,
				bitWidth: curCtx.bitWidth,
				height:   curCtx.height - 1,
			}

			subn, err := cur.links[i].load(ctx, subCtx.bs, subCtx.bitWidth, subCtx.height)
			if err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			err = parallelAddAllTrackedWithNodeSink(ctx, subCtx, subn, offs, subPath, t.sink, results)
			if err != nil {
				return err
			}
			continue
		}

		// Both previous and current have links to diff
		if prev.links[i].cid == cur.links[i].cid {
			continue
		}

		prevSubCtx := &nodeContext{
			bs:       prevCtx.bs,
			bitWidth: prevCtx.bitWidth,
			height:   prevCtx.height - 1,
		}

		prevSubn, err := prev.links[i].load(ctx, prevSubCtx.bs, prevSubCtx.bitWidth, prevSubCtx.height)
		if err != nil {
			return err
		}

		curSubCtx := &nodeContext{
			bs:       curCtx.bs,
			bitWidth: curCtx.bitWidth,
			height:   curCtx.height - 1,
		}

		curSubn, err := cur.links[i].load(ctx, curSubCtx.bs, curSubCtx.bitWidth, curSubCtx.height)
		if err != nil {
			return err
		}

		offs := offset + (uint64(i) * subCount)

		t.enqueueTask(&trackedTask{
			task: task{
				prevCtx: prevSubCtx,
				curCtx:  curSubCtx,
				prev:    prevSubn,
				cur:     curSubn,
				offset:  offs,
			},
			path: subPath,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
