package io.nsq.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFramedDecoder;
import io.netty.handler.codec.compression.SnappyFramedEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.nsq.Connection;
import io.nsq.frames.NSQFrame;
import io.nsq.frames.ResponseFrame;
import org.apache.logging.log4j.LogManager;

public class NSQFeatureDedectionHandler extends SimpleChannelInboundHandler<NSQFrame> {

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final NSQFrame msg) throws Exception {
        LogManager.getLogger(this).info("IdentifyResponse: " + new String(msg.getData()));
        boolean reinstallDefaultDecoder = true;
        if (msg instanceof ResponseFrame) {
            ResponseFrame response = (ResponseFrame) msg;
            ChannelPipeline pipeline = ctx.channel().pipeline();
            final Connection con = ctx.channel().attr(Connection.STATE).get();
            if (response.getMessage().equals("OK")) {
                return;
            }
            if (response.getMessage().contains("\"snappy\":true")) {
                LogManager.getLogger(this).info("Adding snappy to pipline");
                reinstallDefaultDecoder = false;
                pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyFramedEncoder());
                pipeline.replace("LengthFieldBasedFrameDecoder", "SnappyDecoder", new SnappyFramedDecoder());
            }
            if (response.getMessage().contains("\"deflate\":true")) {
                LogManager.getLogger(this).info("Adding deflate to pipline");
                reinstallDefaultDecoder = false;
                pipeline.addBefore("NSQEncoder", "DeflateEncoder", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE, con.getConfig().getDeflateLevel()));
                pipeline.replace("LengthFieldBasedFrameDecoder", "DeflateDecoder", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE));
            }
            if (response.getMessage().contains("version")) {
                // ok we read only the the first message to set up the pipline, ejecting now!
                pipeline.remove(this);
                if (reinstallDefaultDecoder) {
                    pipeline.replace("LengthFieldBasedFrameDecoder", "LengthFieldBasedFrameDecoder",
                            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES));
                }
            }
        }
        ctx.fireChannelRead(msg);
    }
}
