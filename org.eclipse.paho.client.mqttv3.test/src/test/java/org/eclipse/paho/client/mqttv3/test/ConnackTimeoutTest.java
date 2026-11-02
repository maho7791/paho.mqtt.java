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
package org.eclipse.paho.client.mqttv3.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.test.utilities.FakeMqttBroker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A connect attempt whose TCP connection comes up but whose CONNACK never arrives must fail after
 * connectionTimeout (issue #511). Runs without an external broker.
 */
public class ConnackTimeoutTest {

	private FakeMqttBroker broker;
	private MqttAsyncClient client;

	@Before
	public void setUp() throws Exception {
		broker = new FakeMqttBroker();
	}

	@After
	public void tearDown() throws Exception {
		if (client != null) {
			try {
				client.disconnectForcibly(0, 100);
			} catch (MqttException e) {
				// not connected
			}
			client.close(true);
		}
		broker.close();
	}

	private static MqttConnectOptions options(int connectionTimeout) {
		MqttConnectOptions options = new MqttConnectOptions();
		options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1); // no fallback attempt with 3.1
		options.setConnectionTimeout(connectionTimeout);
		options.setKeepAliveInterval(30);
		return options;
	}

	private static class Outcome implements IMqttActionListener {
		final CountDownLatch done = new CountDownLatch(1);
		final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();

		public void onSuccess(IMqttToken asyncActionToken) {
			done.countDown();
		}

		public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
			failure.set(exception);
			done.countDown();
		}
	}

	@Test
	public void testConnectFailsWhenConnackIsMissing() throws Exception {
		broker.setAnswering(false);
		client = new MqttAsyncClient(broker.getServerURI(), "connack-timeout", new MemoryPersistence());
		Outcome outcome = new Outcome();
		client.connect(options(1), null, outcome);

		Assert.assertTrue("Connect callback not called although no CONNACK arrived", outcome.done.await(5, TimeUnit.SECONDS));
		Assert.assertTrue("Connect did not fail", outcome.failure.get() instanceof MqttException);
		Assert.assertEquals(MqttException.REASON_CODE_CLIENT_TIMEOUT, ((MqttException) outcome.failure.get()).getReasonCode());
		Assert.assertFalse(client.isConnected());

		// The client is not left in CONNECTING: the next attempt goes through.
		broker.setAnswering(true);
		client.connect(options(1)).waitForCompletion(5000);
		Assert.assertTrue(client.isConnected());
	}

	@Test
	public void testReconnectContinuesWhenConnackIsMissing() throws Exception {
		client = new MqttAsyncClient(broker.getServerURI(), "connack-timeout-reconnect", new MemoryPersistence());
		MqttConnectOptions options = options(1);
		options.setAutomaticReconnect(true);
		options.setMaxReconnectDelay(2000);
		client.connect(options).waitForCompletion(5000);
		Assert.assertEquals(1, broker.getConnectCount());

		broker.setAnswering(false);
		broker.dropConnections();

		boolean retried = waitFor(new Condition() {
			public boolean met() {
				return broker.getConnectCount() >= 3;
			}
		}, 15000);
		Assert.assertTrue("Reconnect cycle stopped after an attempt without CONNACK, CONNECT count: "
				+ broker.getConnectCount(), retried);

		broker.setAnswering(true);
		Assert.assertTrue("Client did not reconnect once the server answered again", waitFor(new Condition() {
			public boolean met() {
				return client.isConnected();
			}
		}, 10000));
	}

	@Test
	public void testZeroConnectionTimeoutKeepsWaiting() throws Exception {
		broker.setAnswering(false);
		client = new MqttAsyncClient(broker.getServerURI(), "connack-no-timeout", new MemoryPersistence());
		Outcome outcome = new Outcome();
		client.connect(options(0), null, outcome);

		Assert.assertFalse("connectionTimeout=0 must keep waiting for the CONNACK", outcome.done.await(2500, TimeUnit.MILLISECONDS));
	}

	private interface Condition {
		boolean met();
	}

	private static boolean waitFor(Condition condition, long timeoutMillis) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			if (condition.met()) {
				return true;
			}
			Thread.sleep(100);
		}
		return condition.met();
	}
}
