// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![deny(warnings, rust_2018_idioms)]

use criterion::*;
use futures::Async;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

const NUM_SPAWN: usize = 10_000;
const NUM_YIELD: usize = 1_000;
const TASKS_PER_CPU: usize = 50;

struct Fn<F>(F);

impl<F> Future for Fn<F>
where
    F: FnMut(&mut Context<'_>) -> Poll<()> + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.get_mut().0(cx)
    }
}

impl<F> futures::future::Future for Fn<F>
where
    F: FnMut() -> Poll<()>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        match self.0() {
            Poll::Ready(()) => return Ok(Async::Ready(())),
            Poll::Pending => return Ok(Async::NotReady),
        }
    }
}

mod tokio_threadpool {
    use super::Fn;
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};
    use std::task::Poll;
    use tokio_threadpool::*;

    pub fn spawn_many(b: &mut criterion::Bencher<'_>) {
        let threadpool = ThreadPool::new();

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                threadpool.spawn(Fn(move || {
                    if 1 == rem.fetch_sub(1, SeqCst) {
                        tx.send(()).unwrap();
                    }

                    Poll::Ready(())
                }));
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher<'_>) {
        let threadpool = ThreadPool::new();
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                threadpool.spawn(Fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Poll::Ready(())
                    } else {
                        // Notify the current task
                        futures::task::current().notify();

                        // Not ready
                        Poll::Pending
                    }
                }));
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

// In this case, CPU pool completes the benchmark faster, but this is due to how
// CpuPool currently behaves, starving other futures. This completes the
// benchmark quickly but results in poor runtime characteristics for a thread
// pool.
//
// See rust-lang-nursery/futures-rs#617
//
mod cpupool {
    use super::Fn;
    use futures_cpupool::*;
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};
    use std::task::Poll;

    pub fn spawn_many(b: &mut criterion::Bencher<'_>) {
        let pool = CpuPool::new(num_cpus::get());

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn(Fn(move || {
                    if 1 == rem.fetch_sub(1, SeqCst) {
                        tx.send(()).unwrap();
                    }

                    Poll::Ready(())
                }))
                .forget();
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher<'_>) {
        let pool = CpuPool::new(num_cpus::get());
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                pool.spawn(Fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Poll::Ready(())
                    } else {
                        // Notify the current task
                        futures::task::current().notify();

                        // Not ready
                        Poll::Pending
                    }
                }))
                .forget()
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

mod thread_pool_callback {
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};
    use yatp::pool::CloneRunnerBuilder;
    use yatp::queue::{self, Extras};
    use yatp::task::callback::{Handle, Runner, TaskCell};
    use yatp::Builder;

    pub fn spawn_many(b: &mut criterion::Bencher<'_>) {
        let runner = Runner::default();
        let pool = Builder::new("bench-spawn-many-callback")
            .build(queue::simple, CloneRunnerBuilder(runner));

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn(TaskCell::new_once(
                    move |_| {
                        if 1 == rem.fetch_sub(1, SeqCst) {
                            tx.send(()).unwrap();
                        }
                    },
                    Extras::simple_default(),
                ));
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher<'_>) {
        let runner = Runner::default();
        let pool = Builder::new("bench-yield-many-callback")
            .build(queue::simple, CloneRunnerBuilder(runner));
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                fn sub_rem(c: &mut Handle<'_>, rem: &mut usize, tx: &mpsc::SyncSender<()>) {
                    *rem -= 1;
                    if *rem == 0 {
                        tx.send(()).unwrap();
                    } else {
                        c.set_rerun(true);
                    }
                }

                pool.spawn(TaskCell::new_mut(
                    move |c| sub_rem(c, &mut rem, &tx),
                    Extras::simple_default(),
                ))
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

mod thread_pool_future {
    use super::Fn;
    use num_cpus;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{mpsc, Arc};
    use std::task::{Context, Poll};
    use yatp::pool::CloneRunnerBuilder;
    use yatp::queue::{self, Extras};
    use yatp::task::future::{Runner, TaskCell};
    use yatp::Builder;

    pub fn spawn_many(b: &mut criterion::Bencher<'_>) {
        let r = Runner::new(3);
        let pool = Builder::new("test_basic").build(queue::simple, CloneRunnerBuilder(r));

        let (tx, rx) = mpsc::sync_channel(10);
        let rem = Arc::new(AtomicUsize::new(0));
        let remote = pool.remote();

        b.iter(move || {
            rem.store(super::NUM_SPAWN, SeqCst);

            for _ in 0..super::NUM_SPAWN {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn(TaskCell::new(
                    Fn(move |_: &mut Context<'_>| {
                        if 1 == rem.fetch_sub(1, SeqCst) {
                            tx.send(()).unwrap();
                        }

                        Poll::Ready(())
                    }),
                    remote.clone(),
                    Extras::multilevel_default(),
                ));
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn yield_many(b: &mut criterion::Bencher<'_>) {
        let tasks = super::TASKS_PER_CPU * num_cpus::get();
        let r = Runner::new(3);
        let pool = Builder::new("test_basic").build(queue::simple, CloneRunnerBuilder(r));
        let remote = pool.remote();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::NUM_YIELD;
                let tx = tx.clone();

                pool.spawn(TaskCell::new(
                    Fn(move |cx: &mut Context<'_>| {
                        rem -= 1;

                        if rem == 0 {
                            tx.send(()).unwrap();
                            Poll::Ready(())
                        } else {
                            // Notify the current task
                            cx.waker().wake_by_ref();

                            // Not ready
                            Poll::Pending
                        }
                    }),
                    remote.clone(),
                    Extras::multilevel_default(),
                ));
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

fn spawn_many(b: &mut Criterion) {
    b.bench(
        "spawn_many",
        ParameterizedBenchmark::new(
            "thread_pool_future",
            |b, _| thread_pool_future::spawn_many(b),
            &[()],
        )
        .with_function("thread_pool_callback", |b, _| {
            thread_pool_callback::spawn_many(b)
        })
        .with_function("cpupool", |b, _| cpupool::spawn_many(b))
        .with_function("tokio_threadpool", |b, _| tokio_threadpool::spawn_many(b)),
    );
}

fn yield_many(b: &mut Criterion) {
    b.bench(
        "yield_many",
        ParameterizedBenchmark::new(
            "thread_pool_future",
            |b, _| thread_pool_future::yield_many(b),
            &[()],
        )
        .with_function("thread_pool_callback", |b, _| {
            thread_pool_callback::yield_many(b)
        })
        .with_function("cpupool", |b, _| cpupool::yield_many(b))
        .with_function("tokio_threadpool", |b, _| tokio_threadpool::yield_many(b)),
    );
}

criterion_group!(benches, spawn_many, yield_many);

criterion_main!(benches);
