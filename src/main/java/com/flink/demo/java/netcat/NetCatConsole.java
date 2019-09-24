package com.flink.demo.java.netcat;

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
        String host = "localhost";
        int port = 9999;

        if (args.length == 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (Exception e) {
                String help = "(1)no arguments,listen the default port:9999\n\"java nc\"\n\n(2)or listen customer port:8888\n\"java nc 8888\"";
                System.out.println(help);
                return;
            }
        }
//        List<String> messages = generateMessages();
        List<String> messages = generateMessages1();
        sendMessages(host, port, messages);
    }

    public static List<String> generateMessages() {
        List<String> messageList = new ArrayList<String>();
        for (int i = 0; i < 2000; i++) {
            Random random = new Random();
            int len = random.nextInt(5);
            if (len == 0) {
                continue;
            }

            List<String> words = new ArrayList<>();
            for (int j = 0; j < len; j++) {
                int initialInt = Integer.valueOf('a');
                StringBuffer messageStringBuffer = new StringBuffer();
                for (int k = 0; k < len; k++) {
                    int currentInt = initialInt + k;
                    messageStringBuffer.append((char) currentInt);
                }
                words.add(messageStringBuffer.toString());
            }
            messageList.add(String.join(" ", words));
        }
        return messageList;
    }

    public static List<String> generateMessages1() {
        List<String> messageList = new ArrayList<>();
        // Event Time: 2018-10-01 10:11:22.000
        messageList.add("0001,1538359882000");
        messageList.add("0001,1538359883000");
        messageList.add("0001,1538359884000");
        messageList.add("0001,1538359885000");
        messageList.add("0001,1538359886000");
        messageList.add("0001,1538359887000");
        messageList.add("0001,1538359888000");
        messageList.add("0001,1538359893000");
        // Event Time: 2018-10-01 10:11:34.000
        // CurrentMaxTimeStamp: 2018-10-01 10:11:34.000
        // WaterMark: 2018-10-01 10:11:24.000
        // active Window
        messageList.add("0001,1538359894000");
        // Event Time: 2018-10-01 10:11:36.000
        messageList.add("0001,1538359896000");
        // Event Time: 2018-10-01 10:11:37.000
        messageList.add("0001,1538359897000");
        // Event Time: 2018-10-01 10:11:39.000
        messageList.add("0001,1538359899000");
        // Event Time: 2018-10-01 10:11:31.000
        // CurrentMaxTimeStamp: 2018-10-01 10:11:39.000 (no change)
        // WaterMark: 2018-10-01 10:11:29.000 (no change)
        messageList.add("0001,1538359891000");
        // Event Time: 2018-10-01 10:11:43.000
        // CurrentMaxTimeStamp: 2018-10-01 10:11:43.000
        // WaterMark: 2018-10-01 10:11:33.000
        messageList.add("0001,1538359903000");
        // Event Time: 2018-10-01 10:11:44.000
        // CurrentMaxTimeStamp: 2018-10-01 10:11:44.000
        // WaterMark: 2018-10-01 10:11:34.000
//        messageList.add("0001,1538359904000");
        // Event Time: 2018-10-01 10:11:31.000
        messageList.add("0001,1538359891000");
        // Event Time: 2018-10-01 10:11:32.000
        messageList.add("0001,1538359892000");
        messageList.add("0001,1538359889000");
        return messageList;
    }

    public static void sendMessages(String host, int port, List<String> messages) throws IOException {

        Socket client;
        ServerSocket server;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


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

            for (String message : messages) {
                pw.println(message);
                Thread.sleep(1000);
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
