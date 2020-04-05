package com.example.java.netcat;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

public class NetCatWindow extends WindowAdapter implements ActionListener {

    boolean flag = false;
    Socket client;
    JTextArea ta;
    JFrame serverframe;
    JTextField tf;
    PrintWriter pw;
    ServerSocket server;
    static int port = 9999;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public NetCatWindow() {
        try {
            server = new ServerSocket(port);
            this.ServerFrame("服务器端口号" + server.getLocalPort());
            this.server();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void server() throws IOException {
        flag = true;
        while (flag) {
            try {
                client = server.accept();

                System.out.println("[" + sdf.format(new Date()) + "] " + client.getInetAddress() + "已建立连接！");
                // 输入流
                InputStream is = client.getInputStream();
                BufferedReader bri = new BufferedReader(new InputStreamReader(
                        is));
                // 输出流
                OutputStream os = client.getOutputStream();
                // 参数true表示每写一行，PrintWriter缓存就自动溢出，把数据写到目的地
                pw = new PrintWriter(os, true);
                String str;
                while ((str = bri.readLine()) != null) {
                    ta.append(str + "\n");
                    if (str.equals("bye")) {
                        flag = false;
                        break;
                    }
                }
                is.close();
                bri.close();
                os.close();
                pw.close();
            } catch (Exception e) {
                System.out.println("[" + sdf.format(new Date()) + "] connection exit!");
                System.out.println();
            } finally {

            }
        }

    }

    public void actionPerformed(ActionEvent e) {
        ta.append(tf.getText() + "\n");
        pw.println(tf.getText());
        tf.setText("");

    }

    public void windowClosing(WindowEvent e) {
        pw.println("bye");
        System.exit(0);
    }

    public void ServerFrame(String port) {
        serverframe = new JFrame("" + port);
        serverframe.setLayout(new BorderLayout());
        serverframe.setSize(400, 300);
        ta = new JTextArea();
        ta.setEditable(false);
        tf = new JTextField(20);
        JButton send = new JButton("Send");
        send.addActionListener(this);
        tf.addActionListener(this);
        Container container = serverframe.getContentPane();
        container.add(ta, BorderLayout.CENTER);
        JPanel p = new JPanel();
        p.add(tf);
        p.add(send);
        container.add(p, BorderLayout.SOUTH);
        serverframe.addWindowListener(this);
        serverframe.setVisible(true);
    }

    public static void main(String args[]) {
        if (args.length == 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (Exception e) {
                String help = "(1)no arguments,listen the default port:9999\n\"java nc\"\n\n(2)or listen customer port:8888\n\"java nc 8888\"";
                System.out.println(help);
                return;
            }
        }
        new NetCatWindow();
    }
}
