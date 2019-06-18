package com.github.dadoself.rx.utils

import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleSource
import java.util.*
import kotlin.collections.ArrayList


fun <T, R> Observable<T>.flatMapSingleSequentially(mapper: (T) -> SingleSource<R>): Single<List<R>> =
    flatMapSingleConcurrently(1, mapper)

fun <T, R> Single<out Iterable<T>>.flatMapSequentially(mapper: (T) -> SingleSource<R>): Single<List<R>> =
    flatMapConcurrently(1, mapper)

fun <T, R> Single<out Iterator<T>>.flatMapSequentiallyOnIterator(mapper: (T) -> SingleSource<R>): Single<List<R>> =
    flatMapConcurrentlyOnIterator(1, mapper)

fun <T, R> Observable<T>.flatMapSingleConcurrently(concurrently: Int, mapper: (T) -> SingleSource<R>): Single<List<R>> =
    toList().flatMapConcurrently(concurrently, mapper)

fun <T, R> Single<out Iterable<T>>.flatMapConcurrently(
    concurrently: Int,
    mapper: (T) -> SingleSource<R>
): Single<List<R>> =
    map { it.iterator() }.flatMapConcurrentlyOnIterator(concurrently, mapper)

fun <T, R> Single<out Iterator<T>>.flatMapConcurrentlyOnIterator(
    concurrently: Int,
    mapper: (T) -> SingleSource<R>
): Single<List<R>> =
    map { ArrayList<SingleSource<R>>() to it }
    .map { pair ->
        val concurrently = Math.max(concurrently, 1)
        while (pair.first.size < concurrently && pair.second.hasNext())
            pair.first.add(pair.second.next().let(mapper))
        pair
    }
    .flatMap { pair ->
        val results = LinkedList<R>()
        var sequence = Single.just(results)

        pair.first.forEach { op: SingleSource<out R> ->
            sequence = sequence.flatMap { op }
                .map {
                    results.add(it)
                    results
                }
        }

        if (pair.second.hasNext()) {
            sequence = sequence.map { pair.second }
                .flatMapConcurrentlyOnIterator(concurrently, mapper)
                .map {
                    results.addAll(it)
                    results
                }
        }

        sequence
    }
