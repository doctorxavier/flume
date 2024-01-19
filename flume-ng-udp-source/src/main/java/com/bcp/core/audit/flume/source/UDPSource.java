package com.bcp.core.audit.flume.source;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SyslogSourceConfigurationConstants;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDPSource extends AbstractSource implements EventDrivenSource, Configurable {

	// Default Min size
	public static final int		DEFAULT_MIN_SIZE		= 2048;
	public static final int		DEFAULT_INITIAL_SIZE	= DEFAULT_MIN_SIZE;
		
	private static final Logger	LOGGER					= LoggerFactory.getLogger(UDPSource.class);

	private static final int SECONDS = 60;
	
	private int					port;
	
	//CHECKSTYLE:OFF
	// 64k is max allowable in RFC 5426
	private int					maxsize					= 1 << 16;
	//CHECKSTYLE:ON
	
	private String				host;

	private Channel				nettyChannel;
	private CounterGroup		counterGroup			= new CounterGroup();

	@Override
	public void configure(Context context) {
		Configurables.ensureRequiredNonNull(context, SyslogSourceConfigurationConstants.CONFIG_PORT);
		port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
		host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
	}

	@Override
	public void start() {
		// setup Netty server
		ConnectionlessBootstrap serverBootstrap = new ConnectionlessBootstrap(new OioDatagramChannelFactory(Executors.newCachedThreadPool()));

		final UDPHandler handler = new UDPHandler();

		serverBootstrap.setOption("receiveBufferSizePredictorFactory", new AdaptiveReceiveBufferSizePredictorFactory(DEFAULT_MIN_SIZE, DEFAULT_INITIAL_SIZE, maxsize));
		serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() {
				return Channels.pipeline(handler);
			}
		});

		if (host == null) {
			nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
		} else {
			nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
		}

		super.start();
	}

	@Override
	public void stop() {
		LOGGER.info("Syslog UDP Source stopping...");
		LOGGER.info("Metrics:{}", counterGroup);
		if (nettyChannel != null) {
			nettyChannel.close();
			try {
				nettyChannel.getCloseFuture().await(SECONDS, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOGGER.warn("netty server stop interrupted", e);
			} finally {
				nettyChannel = null;
			}
		}

		super.stop();
	}

	public class UDPHandler extends SimpleChannelHandler {

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent mEvent) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Message received.");
			}
			Event e = null;
			try {
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream(maxsize);
				ChannelBuffer in = (ChannelBuffer) mEvent.getMessage();

				try {
					while (in.readable()) {
						outputStream.write(in.readByte());
					}
					e = EventBuilder.withBody(outputStream.toByteArray());
				} catch (Exception ex) {
					LOGGER.error("Error reading message", ex);
					return;
				//CHECKSTYLE:OFF
				} finally {
					// no-op
				}
				//CHECKSTYLE:ON

				if (e == null) {
					LOGGER.error("Event created is null.");
					return;
				}
				getChannelProcessor().processEvent(e);
				counterGroup.incrementAndGet("events.success");
			} catch (ChannelException ex) {
				counterGroup.incrementAndGet("events.dropped");
				LOGGER.error("Error writting to channel", ex);
				return;
			}
		}
	}

}
