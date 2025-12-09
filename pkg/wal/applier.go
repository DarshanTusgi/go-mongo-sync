package wal

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	
	"go.mongodb.org/mongo-driver/bson"
)

// ApplyFunc is the callback function for applying WAL entries to MongoDB
// It receives the database, collection, and documents to apply
type ApplyFunc func(ctx context.Context, database, collection string, documents []byte) error

// Applier manages async workers that apply WAL entries to MongoDB
type Applier struct {
	wal         *WAL
	applyFunc   ApplyFunc
	
	// Work queue
	workQueue   chan *Entry
	
	// Workers
	workers     []*worker
	workerCount int
	
	// Stats
	appliedCount uint64
	errorCount   uint64
	
	// Lifecycle
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	closed      atomic.Bool
}

// worker represents a single MongoDB writer
type worker struct {
	id        int
	applier   *Applier
	ctx       context.Context
}

// NewApplier creates a new applier with async MongoDB workers
func NewApplier(wal *WAL, applyFunc ApplyFunc) *Applier {
	ctx, cancel := context.WithCancel(context.Background())
	
	applier := &Applier{
		wal:         wal,
		applyFunc:   applyFunc,
		workQueue:   make(chan *Entry, ApplyQueueSize),
		workerCount: ApplyWorkerCount,
		ctx:         ctx,
		cancel:      cancel,
	}
	
	// Start workers
	applier.workers = make([]*worker, ApplyWorkerCount)
	for i := 0; i < ApplyWorkerCount; i++ {
		w := &worker{
			id:      i,
			applier: applier,
			ctx:     ctx,
		}
		applier.workers[i] = w
		
		applier.wg.Add(1)
		go w.run()
	}
	
	log.Printf("✅ APPLIER STARTED: %d workers, queue size %d", ApplyWorkerCount, ApplyQueueSize)
	
	return applier
}

// Submit submits an entry for async application
func (a *Applier) Submit(entry *Entry) error {
	if a.closed.Load() {
		return fmt.Errorf("applier is closed")
	}
	
	select {
	case a.workQueue <- entry:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("work queue full (timeout)")
	}
}

// RecoverPendingEntries replays all pending WAL entries on startup
func (a *Applier) RecoverPendingEntries() error {
	pending, err := a.wal.GetPendingEntries()
	if err != nil {
		return fmt.Errorf("get pending entries: %w", err)
	}
	
	if len(pending) == 0 {
		log.Printf("✅ RECOVERY: No pending entries to replay")
		return nil
	}
	
	log.Printf("🔄 RECOVERY: Replaying %d pending entries...", len(pending))
	
	// Submit all pending entries
	for _, entry := range pending {
		if err := a.Submit(entry); err != nil {
			return fmt.Errorf("submit entry %d: %w", entry.EntryID, err)
		}
	}
	
	log.Printf("✅ RECOVERY: All %d pending entries submitted for replay", len(pending))
	
	return nil
}

// worker.run processes entries from the work queue
func (w *worker) run() {
	defer w.applier.wg.Done()
	
	log.Printf("🔧 WORKER %d: Started", w.id)
	
	for {
		select {
		case entry := <-w.applier.workQueue:
			w.processEntry(entry)
			
		case <-w.ctx.Done():
			log.Printf("🛑 WORKER %d: Stopping", w.id)
			return
		}
	}
}

// processEntry applies a single entry to MongoDB
func (w *worker) processEntry(entry *Entry) {
	startTime := time.Now()
	
	// Create context with timeout (60s for large batches)
	ctx, cancel := context.WithTimeout(w.ctx, 60*time.Second)
	defer cancel()
	
	// Call apply function (writes to MongoDB)
	err := w.applier.applyFunc(ctx, entry.Database, entry.Collection, entry.Documents)
	
	duration := time.Since(startTime)
	
	if err != nil {
		atomic.AddUint64(&w.applier.errorCount, 1)
		log.Printf("⚠️  WORKER %d: FAILED to apply entry %d (%s.%s) - %v [%dms]", 
			w.id, entry.EntryID, entry.Database, entry.Collection, err, duration.Milliseconds())
		
		// TODO: Implement retry logic with exponential backoff
		// For now, log error and continue
		return
	}
	
	// Mark as applied in WAL
	if markErr := w.applier.wal.MarkApplied(entry.EntryID); markErr != nil {
		log.Printf("⚠️  WORKER %d: Failed to mark entry %d as applied: %v", w.id, entry.EntryID, markErr)
	}
	
	atomic.AddUint64(&w.applier.appliedCount, 1)
	
	// Parse document count for logging
	docCount := w.countDocuments(entry.Documents)
	
	log.Printf("✅ WORKER %d: Applied entry %d (%s.%s) - %d docs [%dms]", 
		w.id, entry.EntryID, entry.Database, entry.Collection, docCount, duration.Milliseconds())
}

// countDocuments counts the number of documents in the BSON array
func (w *worker) countDocuments(documents []byte) int {
	// Parse as BSON array
	var docs []bson.Raw
	if err := bson.Unmarshal(documents, &docs); err != nil {
		return 0
	}
	return len(docs)
}

// Stats returns applier statistics
func (a *Applier) Stats() (applied, errors uint64) {
	return atomic.LoadUint64(&a.appliedCount), atomic.LoadUint64(&a.errorCount)
}

// Close stops all workers and waits for completion
func (a *Applier) Close() error {
	if !a.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}
	
	log.Printf("🛑 APPLIER: Stopping workers...")
	
	// Cancel context to stop workers
	a.cancel()
	
	// Wait for all workers to finish
	a.wg.Wait()
	
	applied, errors := a.Stats()
	log.Printf("✅ APPLIER CLOSED: %d applied, %d errors", applied, errors)
	
	return nil
}
