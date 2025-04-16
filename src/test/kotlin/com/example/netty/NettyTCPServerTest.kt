package com.example.netty

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.StringDecoder
import io.netty.handler.codec.string.StringEncoder
import kotlinx.coroutines.*
import java.net.ConnectException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

class NettyTCPServerTest : FunSpec({
    val port = 8081
    val maxConnections = 10

    test("Server should start, handle messages, and close properly") {
        runBlocking {
            val server = NettyTCPServer(port, maxConnections)
            val serverDeferred = server.start()

            delay(1000)

            val clientGroup = NioEventLoopGroup()
            val receivedMessages = LinkedBlockingQueue<String>()
            try {
                val bootstrap = Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel::class.java)
                    .handler(object : ChannelInitializer<NioSocketChannel>() {
                        override fun initChannel(ch: NioSocketChannel) {
                            ch.pipeline().addLast(StringDecoder(), StringEncoder(), object : SimpleChannelInboundHandler<String>() {
                                override fun channelRead0(ctx: ChannelHandlerContext, msg: String) {
                                    receivedMessages.add(msg)
                                }
                            })
                        }
                    })

                val channelFuture = bootstrap.connect("localhost", port).sync()
                val channel = channelFuture.channel()

                val testMessage = "Hello, Netty!"
                channel.writeAndFlush("$testMessage\n").await()

                val response = receivedMessages.poll(10, TimeUnit.SECONDS)
                response shouldBe "Server response: $testMessage\n"

                channel.close().sync()
            } finally {
                clientGroup.shutdownGracefully()
                server.shutdown()
                serverDeferred.join()
            }
        }
    }

    test("Server should enforce max connections limit") {
        runBlocking {
            val server = NettyTCPServer(port, maxConnections)
            val serverDeferred = server.start()

            delay(1000)

            val clientGroup = NioEventLoopGroup()
            try {
                val bootstrap = Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel::class.java)
                    .handler(object : ChannelInitializer<NioSocketChannel>() {
                        override fun initChannel(ch: NioSocketChannel) {
                            ch.pipeline().addLast(StringDecoder(), StringEncoder())
                        }
                    })

                val channels = mutableListOf<io.netty.channel.Channel>()
                repeat(maxConnections) {
                    channels.add(bootstrap.connect("localhost", port).sync().channel())
                }
                server.getActiveConnections() shouldBe maxConnections

                val extraChannelFuture = bootstrap.connect("localhost", port)
                delay(500)
                extraChannelFuture.isSuccess shouldBe false
                server.getActiveConnections() shouldBe maxConnections

                channels.forEach { it.close().sync() }
            } finally {
                clientGroup.shutdownGracefully()
                server.shutdown()
                serverDeferred.join()
            }
        }
    }

    test("Server should handle high load with optimized coroutines") {
        runBlocking {
            val server = NettyTCPServer(port, maxConnections)
            val serverDeferred = server.start()

            delay(1000)

            val clientGroup = NioEventLoopGroup()
            val clients = mutableListOf<io.netty.channel.Channel>()
            val receivedMessages = LinkedBlockingQueue<String>()
            try {
                val bootstrap = Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel::class.java)
                    .handler(object : ChannelInitializer<NioSocketChannel>() {
                        override fun initChannel(ch: NioSocketChannel) {
                            ch.pipeline().addLast(StringDecoder(), StringEncoder(), object : SimpleChannelInboundHandler<String>() {
                                override fun channelRead0(ctx: ChannelHandlerContext, msg: String) {
                                    receivedMessages.add(msg)
                                }
                            })
                        }
                    })

                repeat(5) {
                    clients.add(bootstrap.connect("localhost", port).sync().channel())
                }

                val timeTaken = measureTimeMillis {
                    clients.forEach { channel ->
                        launch {
                            repeat(1000) {
                                channel.writeAndFlush("Test message\n").await()
                            }
                        }
                    }
                    repeat(5 * 1000) {
                        receivedMessages.poll(10, TimeUnit.SECONDS) shouldBe "Server response: Test message\n"
                    }
                }
                println("Time taken for 5000 messages: $timeTaken ms")
                server.getActiveConnections() shouldBe 5

                clients.forEach { it.close().sync() }
            } finally {
                clientGroup.shutdownGracefully()
                server.shutdown()
                serverDeferred.join()
            }
        }
    }

    test("Server should handle backpressure under high message rate") {
        runBlocking {
            val server = NettyTCPServer(port, maxConnections)
            val serverDeferred = server.start()

            delay(1000)

            val clientGroup = NioEventLoopGroup()
            val receivedMessages = LinkedBlockingQueue<String>()
            try {
                val bootstrap = Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel::class.java)
                    .handler(object : ChannelInitializer<NioSocketChannel>() {
                        override fun initChannel(ch: NioSocketChannel) {
                            ch.pipeline().addLast(StringDecoder(), StringEncoder(), object : SimpleChannelInboundHandler<String>() {
                                override fun channelRead0(ctx: ChannelHandlerContext, msg: String) {
                                    receivedMessages.add(msg)
                                }
                            })
                        }
                    })

                val channel = bootstrap.connect("localhost", port).sync().channel()

                val timeTaken = measureTimeMillis {
                    repeat(1000) {
                        channel.writeAndFlush("Test message\n").await()
                    }
                    repeat(1000) {
                        receivedMessages.poll(10, TimeUnit.SECONDS) shouldBe "Server response: Test message\n"
                    }
                }
                println("Time taken for 1000 messages with backpressure: $timeTaken ms")

                channel.close().sync()
            } finally {
                clientGroup.shutdownGracefully()
                server.shutdown()
                serverDeferred.join()
            }
        }
    }

    test("Server should handle cancellation during high load") {
        runBlocking {
            val server = NettyTCPServer(port, maxConnections)
            val serverDeferred = server.start()

            delay(1000)

            val clientGroup = NioEventLoopGroup()
            val clients = mutableListOf<io.netty.channel.Channel>()
            val receivedMessages = LinkedBlockingQueue<String>()
            try {
                val bootstrap = Bootstrap()
                    .group(clientGroup)
                    .channel(NioSocketChannel::class.java)
                    .handler(object : ChannelInitializer<NioSocketChannel>() {
                        override fun initChannel(ch: NioSocketChannel) {
                            ch.pipeline().addLast(StringDecoder(), StringEncoder(), object : SimpleChannelInboundHandler<String>() {
                                override fun channelRead0(ctx: ChannelHandlerContext, msg: String) {
                                    receivedMessages.add(msg)
                                }
                            })
                        }
                    })

                repeat(5) {
                    clients.add(bootstrap.connect("localhost", port).sync().channel())
                }

                val sendJob = launch {
                    clients.forEach { channel ->
                        repeat(1000) {
                            channel.writeAndFlush("Test message\n").await()
                        }
                    }
                }

                delay(500)
                server.shutdown()
                serverDeferred.join()

                val connectFuture = bootstrap.connect("localhost", port)
                assertThrows<ConnectException> {
                    connectFuture.sync()
                }

                sendJob.cancelAndJoin()
                clients.forEach { it.close().sync() }
            } finally {
                clientGroup.shutdownGracefully()
            }
        }
    }
})

inline fun <reified T : Throwable> assertThrows(block: () -> Unit) {
    try {
        block()
        throw AssertionError("Expected ${T::class.java.name} but no exception was thrown")
    } catch (e: Throwable) {
        if (e !is T) throw AssertionError("Expected ${T::class.java.name} but got ${e.javaClass.name}")
    }
}