/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.test.ignite.performance;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Warmup(iterations = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode({ Mode.AverageTime })
public class MHBenchmark {

	public static final long SIZE = 100_000;

	@State(Scope.Benchmark)
	public static class IgniteInstance {

		volatile Ignite instance;
		volatile IgniteCache<String, ValueObject> cache;
		volatile IgniteCache<String, BinaryObject> cacheAsBinary;
		@Setup
		public void setup() {
			instance = Ignition.start();
			System.setProperty("IGNITE_BINARY_SORT_OBJECT_FIELDS", "true"); 
			CacheConfiguration<String, ValueObject> conf = new CacheConfiguration<>("ignite-config.xml");
			conf.setCacheMode(CacheMode.LOCAL);
			conf.setName("test");
			conf.setCopyOnRead(false);
			conf.setTypes(String.class, ValueObject.class);
	
			conf.setAtomicityMode(CacheAtomicityMode.ATOMIC);
			conf.setStatisticsEnabled(false);
			conf.setBackups(0);
			conf.setEventsDisabled(true);
			conf.setOnheapCacheEnabled(true);
			conf.setStoreByValue(true);

			
			cache = instance.createCache(conf);
			cacheAsBinary = cache.withNoRetries().withSkipStore().withKeepBinary();
			for (int i = 0; i < SIZE; i++) {
				cache.put("key_" + i, ValueObject.createNew(i));
			}
		}

		@TearDown
		public void tearDown() {
			instance.close();
		}
	}

	@State(Scope.Benchmark)
	public static class ValueObject {
		private static Random rn = new Random();

		private String field1;
		private Integer field2;
		private String field3;
		private Integer field4;
		private String field5;
		private Integer field6;
		private String field7;
		private Integer field8;
		private String field9;
		private Integer field10;
		private String field11;
		private Integer field12;
		private String field13;
		private Integer field14;
		private String field15;
		private Integer field16;
		private String field17;
		private Integer field18;
		private String field19;
		private Integer field20;
		private String field21;
		private Integer field22;
		private String field23;
		private Integer field24;
		private String field25;
		private Integer field26;
		private String field27;
		private Integer field28;
		private String field29;
		private Integer field30;
		private String field31;
		private Integer field32;	

		public static ValueObject createNew(Integer i) {
			ValueObject o = new ValueObject();
			o.field1 = "key_" + i;
			o.field2 = rn.nextInt();
			o.field3 = UUID.randomUUID().toString();
			o.field4 = rn.nextInt();
			o.field5 = UUID.randomUUID().toString();
			o.field6 = rn.nextInt();
			o.field7 = UUID.randomUUID().toString();
			o.field8 = rn.nextInt();
			o.field9 = UUID.randomUUID().toString();
			o.field10 = rn.nextInt();
			o.field11 = UUID.randomUUID().toString();
			o.field12 = rn.nextInt();
			o.field13 = UUID.randomUUID().toString();
			o.field14 = rn.nextInt();
			o.field15 = UUID.randomUUID().toString();
			o.field16 = rn.nextInt();
			o.field17 = UUID.randomUUID().toString();
			o.field18 = rn.nextInt();
			o.field19 = UUID.randomUUID().toString();
			o.field20 = rn.nextInt();
			o.field21 = UUID.randomUUID().toString();
			o.field22 = rn.nextInt();
			o.field23 = UUID.randomUUID().toString();
			o.field24 = rn.nextInt();
			o.field25 = UUID.randomUUID().toString();
			o.field26 = rn.nextInt();
			o.field27 = UUID.randomUUID().toString();
			o.field28 = rn.nextInt();
			o.field29 = UUID.randomUUID().toString();
			o.field30 = rn.nextInt();
			o.field31 = UUID.randomUUID().toString();
			o.field32 = rn.nextInt();			
			return o;
		}
	}

	@State(Scope.Benchmark)
	public static class ConcurrentHaspMapInstance {

		volatile ConcurrentHashMap<String, String> map;

		@Setup
		public void setup() {
			map = new ConcurrentHashMap<>();

			for (int i = 0; i < SIZE; i++) {
				map.put("key_" + i, "value_" + i);
			}
		}
	}

	@State(Scope.Thread)
	public static class Key {
		final Random r = new Random();
		String value;
		@Setup(Level.Invocation)
		public void setup() {
			value = "key_" + Math.abs(r.nextLong()) % SIZE;
		}
	}

	@Benchmark
	public void igniteRead(IgniteInstance igniteInstance, Key key, Blackhole bh) throws Throwable {
		ValueObject o = igniteInstance.cache.get(key.value);
		if (o == null) {
			System.out.println(key.value);			
			throw new IllegalArgumentException(key.value);
		}
		String field1 = o.field1;
		if(!field1.equals(key.value)) {
			throw new IllegalStateException(key.value);
		};
	}

	@Benchmark
	public void igniteReadKeepBinary(IgniteInstance igniteInstance, Key key, Blackhole bh) throws Throwable {
		BinaryObject o =  igniteInstance.cacheAsBinary.get(key.value);
		if (o == null) {
			System.out.println(key.value);
			throw new IllegalArgumentException(key.value);
		}
		String field1 = (String) o.field("field1");
		if(!field1.equals(key.value)) {
			throw new IllegalStateException(key.value);
		};
	}

//	@Benchmark
//	public void concurrentHashMapRead(ConcurrentHaspMapInstance concurrentHaspMapInstance, Key key, Blackhole bh)
//			throws Throwable {
//		bh.consume(concurrentHaspMapInstance.map.get(key.value));
//	}
//	public static void main(String[] args) {
//		IgniteInstance i = new IgniteInstance();
//		i.setup();
//		BinaryObject o = i.cacheAsBinary.get("key_1000");
//		String t = o.field("field1");
//		System.out.println(t);
//	}

}
