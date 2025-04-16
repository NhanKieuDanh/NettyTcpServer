package com.example.netty

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.EventLoop
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

class ServerHandler(
    private val eventLoop: EventLoop,
    private val onDisconnect: () -> Unit
) : SimpleChannelInboundHandler<String>() {
    private val handlerScope = CoroutineScope(eventLoop.asCoroutineDispatcher() + SupervisorJob())
    private val messageFlow = MutableSharedFlow<String>(replay = 0, extraBufferCapacity = 100, onBufferOverflow = BufferOverflow.SUSPEND)

    init {
        handlerScope.launch {
            messageFlow
                .buffer(100)
                .collect { msg ->
                    try {
                        ensureActive()
                        if (ctx?.channel()?.isActive == true) {
                            println("Received message: $msg")
                            val future = ctx.writeAndFlush("Server response: $msg\n")
                            future.await()
                        }
                    } catch (e: Exception) {
                        e.printStackTrace()
                        ctx?.close()
                    }
                }
        }
    }

    private var ctx: ChannelHandlerContext? = null

    override fun channelRead0(ctx: ChannelHandlerContext, msg: String) {
        this.ctx = ctx
        handlerScope.launch {
            messageFlow.emit(msg)
        }
    }

    override fun channelActive(ctx: ChannelHandlerContext) {
        this.ctx = ctx
        println("Client connected: ${ctx.channel().remoteAddress()}")
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        println("Client disconnected: ${ctx.channel().remoteAddress()}")
        handlerScope.cancel("Channel inactive")
        onDisconnect()
        this.ctx = null
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
        handlerScope.cancel("Exception caught: ${cause.message}")
        this.ctx = null
    }
}

fun EventLoop.asCoroutineDispatcher(): CoroutineDispatcher = object : CoroutineDispatcher() {
    override fun dispatch(context: CoroutineContext, block: Runnable) {
        execute(block)
    }
}