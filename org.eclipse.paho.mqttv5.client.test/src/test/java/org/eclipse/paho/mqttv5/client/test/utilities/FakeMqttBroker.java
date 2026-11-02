/*******************************************************************************
 * Copyright (c) 2026 Data In Motion Consulting GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0
 * and the Eclipse Distribution License is available at
 *   https://www.eclipse.org/org/documents/edl-v10.php
 *
 * Contributors:
 *    Mark Hoffmann - initial API and implementation
 *******************************************************************************/
package org.eclipse.paho.mqttv5.client.test.utilities;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal in-process MQTT server for tests that need to control the server side of a connection.
 * It answers CONNECT and PINGREQ, and can stop answering: it then keeps accepting connections and
 * reading packets but sends nothing, not even a CONNACK, like a half-open or black-holed link.
 * Works for MQTT 3.1, 3.1.1 and 5.0; it never reports a session as present.
 */
public class FakeMqttBroker {

	private final ServerSocket serverSocket;
	private final List<Socket> connections = new CopyOnWriteArrayList<Socket>();
	private final AtomicInteger connects = new AtomicInteger();
	private volatile boolean answering = true;

	public FakeMqttBroker() throws IOException {
		serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
		Thread acceptor = new Thread(new Runnable() {
			public void run() {
				acceptLoop();
			}
		}, "FakeMqttBroker-accept");
		acceptor.setDaemon(true);
		acceptor.start();
	}

	public String getServerURI() {
		return "tcp://127.0.0.1:" + serverSocket.getLocalPort();
	}

	/** When false, the broker accepts connections and reads packets but sends nothing. */
	public void setAnswering(boolean answering) {
		this.answering = answering;
	}

	/** Number of CONNECT packets received so far. */
	public int getConnectCount() {
		return connects.get();
	}

	/** Closes all client connections; the broker keeps accepting new ones. */
	public void dropConnections() {
		for (Socket socket : connections) {
			closeQuietly(socket);
		}
	}

	public void close() {
		closeQuietly(serverSocket);
		dropConnections();
	}

	private void acceptLoop() {
		while (!serverSocket.isClosed()) {
			try {
				final Socket socket = serverSocket.accept();
				connections.add(socket);
				Thread reader = new Thread(new Runnable() {
					public void run() {
						readLoop(socket);
					}
				}, "FakeMqttBroker-connection");
				reader.setDaemon(true);
				reader.start();
			} catch (IOException e) {
				// server socket closed
			}
		}
	}

	private void readLoop(Socket socket) {
		try {
			DataInputStream in = new DataInputStream(socket.getInputStream());
			OutputStream out = socket.getOutputStream();
			int protocolLevel = 4;
			int header;
			while ((header = in.read()) >= 0) {
				byte[] body = new byte[readRemainingLength(in)];
				in.readFully(body);
				int type = header >> 4;
				if (type == 1) { // CONNECT: length-prefixed protocol name, then the protocol level
					protocolLevel = body[2 + (((body[0] & 0xFF) << 8) | (body[1] & 0xFF))];
					connects.incrementAndGet();
					if (answering) {
						out.write(protocolLevel == 5 ? new byte[] { 0x20, 3, 0, 0, 0 } : new byte[] { 0x20, 2, 0, 0 });
					}
				} else if (type == 12) { // PINGREQ
					if (answering) {
						out.write(new byte[] { (byte) 0xD0, 0 });
					}
				} else if (type == 14) { // DISCONNECT
					return;
				}
				out.flush();
			}
		} catch (IOException e) {
			// connection closed
		} finally {
			connections.remove(socket);
			closeQuietly(socket);
		}
	}

	private static int readRemainingLength(DataInputStream in) throws IOException {
		int value = 0;
		int multiplier = 1;
		int digit;
		do {
			digit = in.readUnsignedByte();
			value += (digit & 0x7F) * multiplier;
			multiplier *= 128;
		} while ((digit & 0x80) != 0);
		return value;
	}

	private static void closeQuietly(Closeable closeable) {
		try {
			closeable.close();
		} catch (IOException e) {
			// ignore
		}
	}
}
