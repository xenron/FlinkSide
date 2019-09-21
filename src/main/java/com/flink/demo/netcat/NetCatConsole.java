package com.flink.demo.netcat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.InputStream;
import java.io.BufferedReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class NetCatConsole {

    public static void main(String args[]) throws IOException {

        int port = 9999;
        Socket client;
        ServerSocket server;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<String> messageList = new ArrayList<String>();
        for (int i = 0; i < 2000; i++) {
            Random random = new Random();
            int len = random.nextInt(5);
            if (len == 0) {
                continue;
            }
            int initialInt = Integer.valueOf('a');
            StringBuffer messageStringBuffer = new StringBuffer();
            for (int j = 0; j < len; j++) {
                int currentInt = initialInt + j;
                messageStringBuffer.append((char) currentInt);
            }
            messageList.add(messageStringBuffer.toString());
        }

        if (args.length == 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (Exception e) {
                String help = "(1)no arguments,listen the default port:9999\n\"java nc\"\n\n(2)or listen customer port:8888\n\"java nc 8888\"";
                System.out.println(help);
                return;
            }
        }
        server = new ServerSocket(port);

        InputStream is = null;
        BufferedReader br = null;
        OutputStream os = null;
        PrintWriter pw = null;
        try {
            client = server.accept();

            System.out.println("[" + sdf.format(new Date()) + "] Client IP: " + client.getInetAddress() + ", Connected !");
            // InputStream
            is = client.getInputStream();
            br = new BufferedReader(new InputStreamReader(is));
            // OutputStream
            os = client.getOutputStream();
            // 参数true表示每写一行，PrintWriter缓存就自动溢出，把数据写到目的地
            pw = new PrintWriter(os, true);

            for (String message : messageList) {
                pw.println(message);
                Thread.sleep(200);
            }

        } catch (Exception e) {
            System.out.println("[" + sdf.format(new Date()) + "] connection exit !");
            System.out.println();
        } finally {
            try {
                if (null != br) {
                    br.close();
                }
                if (null != is) {
                    is.close();
                }
                if (null != pw) {
                    pw.close();
                }
                if (null != os) {
                    os.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("[" + sdf.format(new Date()) + "] Completed !");
        }
    }
}
