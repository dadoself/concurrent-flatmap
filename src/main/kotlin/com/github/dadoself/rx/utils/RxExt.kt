package com.github.dadoself.rx.utils

import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleSource
import java.util.*
import kotlin.collections.ArrayList

//inline fun <T, R> Observable<T>.flatMapSingleSequentially(crossinline mapper: (T) -> SingleSource<R>): Single<List<R>>
//        = map { mapper(it) }
//        .toList()
//        .flatMap { operations: List<SingleSource<out R>> ->
//            val results = ArrayList<R>()
//            var sequence = Single.just(results)
//            operations.forEach { op: SingleSource<out R> ->
//                sequence = sequence.flatMap { op }
//                        .map {
//                            results.add(it)
//                            results
//                        }
//            }
//            sequence
//        }

fun <T, R> Observable<T>.flatMapSingleSequentially(mapper: (T) -> SingleSource<R>): Single<List<R>>
        = flatMapSingleConcurrently(mapper, 1)

fun <T, R> Single<out Iterable<T>>.flatMapSequentially(mapper: (T) -> SingleSource<R>): Single<List<R>>
        = flatMapConcurrently(mapper, 1)

fun <T, R> Single<out Iterator<T>>.flatMapSequentiallyOnIterator(mapper: (T) -> SingleSource<R>): Single<List<R>>
        = flatMapConcurrentlyOnIterator(mapper, 1)

fun <T, R> Observable<T>.flatMapSingleConcurrently(mapper: (T) -> SingleSource<R>, concurrently: Int): Single<List<R>>
        = toList()
        .flatMapConcurrently(mapper, concurrently)

fun <T, R> Single<out Iterable<T>>.flatMapConcurrently(mapper: (T) -> SingleSource<R>, concurrently: Int): Single<List<R>>
        = map { it.iterator() }
        .flatMapConcurrentlyOnIterator(mapper, concurrently)

fun <T, R> Single<out Iterator<T>>.flatMapConcurrentlyOnIterator(mapper: (T) -> SingleSource<R>, concurrently: Int): Single<List<R>>
        = map { ArrayList<SingleSource<R>>() to it }
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
                        .flatMapConcurrentlyOnIterator(mapper, concurrently)
                        .map {
                            results.addAll(it)
                            results
                        }
            }

            sequence
        }
