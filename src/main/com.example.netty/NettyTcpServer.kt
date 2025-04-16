import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.*
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.string.StringDecoder
import io.netty.handler.codec.string.StringEncoder
import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class NettyTCPServer(private val port: Int, private val maxConnections: Int = 1000) {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var serverChannel: Channel? = null
    private val isRunning = AtomicBoolean(false)
    private val activeConnections = AtomicInteger(0)
    private val bossGroup = if (Epoll.isAvailable()) EpollEventLoopGroup(1) else NioEventLoopGroup(1)
    private val workerGroup = if (Epoll.isAvailable()) EpollEventLoopGroup() else NioEventLoopGroup()

    fun start(): Deferred<Unit> = scope.async {
        ensureActive()
        check(!isRunning.getAndSet(true)) { "Server is already running" }

        try {
            val bootstrap = ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(if (Epoll.isAvailable()) EpollServerSocketChannel::class.java else NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel) {
                        if (activeConnections.incrementAndGet() > maxConnections) {
                            ch.close()
                            activeConnections.decrementAndGet()
                            return
                        }
                        val pipeline = ch.pipeline()
                        pipeline.addLast(StringDecoder())
                        pipeline.addLast(StringEncoder())
                        pipeline.addLast(ServerHandler(ch.eventLoop()) { activeConnections.decrementAndGet() })
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)

            val future = bootstrap.bind(port).await()
            serverChannel = future.channel()
            println("Server started on port $port with ${if (Epoll.isAvailable()) "Epoll" else "NIO"}")

            try {
                withContext(NonCancellable) {
                    future.channel().closeFuture().await()
                }
            } catch (e: CancellationException) {
                println("Server start was cancelled")
                cleanupResources()
                throw e
            }
        } catch (e: Exception) {
            println("Server failed to start: ${e.message}")
            cleanupResources()
            throw e
        } finally {
            cleanupResources()
        }
    }

    private suspend fun cleanupResources() {
        try {
            serverChannel?.close()?.await()
            bossGroup.shutdownGracefully().await()
            workerGroup.shutdownGracefully().await()
            isRunning.set(false)
            serverChannel = null
            println("Server resources cleaned up")
        } catch (e: Exception) {
            println("Error during cleanup: ${e.message}")
        }
    }

    fun shutdown() {
        if (isRunning.get()) {
            scope.cancel("Server shutdown requested")
            runBlocking {
                serverChannel?.close()?.await()
            }
        }
    }

    fun isRunning(): Boolean = isRunning.get()
    fun getActiveConnections(): Int = activeConnections.get()
}