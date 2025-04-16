package com.example.netty

import io.netty.channel.ChannelFuture
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

suspend fun ChannelFuture.await() {
    awaitSuspend()
}

private suspend fun ChannelFuture.awaitSuspend(): Unit = suspendCoroutine { cont ->
    addListener {
        if (it.isSuccess) {
            cont.resume(Unit)
        } else {
            cont.resumeWithException(it.cause())
        }
    }
}