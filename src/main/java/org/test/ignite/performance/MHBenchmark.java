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

import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 21, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode({ Mode.AverageTime })
public class MHBenchmark {

	public static final long SIZE = 500_000;
	public static final long MULTIPLE_READ = 1000;
	
	@State(Scope.Benchmark)
	public static class IgniteInstance {

		volatile Ignite instance;
		volatile IgniteCache<Long, ValueObject> cache;
		volatile IgniteCache<Long, BinaryObject> cacheAsBinary;
		volatile IgniteCache<Long, BinaryObject> cacheAsBinaryAndReadUncommitted;
		volatile Random r = new Random();
		volatile BinaryField field;

		@Setup
		public void setup() {
			instance = Ignition.start();
			CacheConfiguration<Long, ValueObject> conf = new CacheConfiguration<>("ignite-config.xml");
			conf.setCacheMode(CacheMode.LOCAL);
			conf.setName("test");
			conf.setCopyOnRead(false);
			conf.setTypes(Long.class, ValueObject.class);

			conf.setAtomicityMode(CacheAtomicityMode.ATOMIC);
			conf.setStatisticsEnabled(false);
			conf.setBackups(0);
			conf.setEventsDisabled(true);
			conf.setOnheapCacheEnabled(true);
			conf.setStoreByValue(true);

			cache = instance.createCache(conf);
			cacheAsBinary = cache.withKeepBinary();
			cacheAsBinaryAndReadUncommitted = cache.withReadUncommitted();

			for (long i = 0; i < SIZE; i++) {
				cache.put(i, ValueObject.createNew(i));
			}

			BinaryType type = instance.binary().type(ValueObject.class);
			field = type.field("field1");
		}

		@TearDown
		public void tearDown() {
			instance.close();
		}
	}

	@State(Scope.Benchmark)
	public static class ValueObject {
		private static Random rn = new Random();

		private Long field1;
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
		private String field33;
		private Integer field34;
		private String field35;
		private Integer field36;
		private String field37;
		private Integer field38;
		private String field39;
		private Integer field40;
		private String field41;
		private Integer field42;
		private String field43;
		private Integer field44;
		private String field45;
		private Integer field46;
		private String field47;
		private Integer field48;
		private String field49;
		private Integer field50;
		private String field51;
		private Integer field52;
		private String field53;
		private Integer field54;
		private String field55;
		private Integer field56;
		private String field57;
		private Integer field58;
		private String field59;
		private Integer field60;
		private String field61;
		private Integer field62;
		private String field63;
		private Integer field64;

		public static ValueObject createNew(Long i) {
			ValueObject o = new ValueObject();
			o.field1 = i;
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
			o.field33 = "key_" + i;
			o.field34 = rn.nextInt();
			o.field35 = UUID.randomUUID().toString();
			o.field36 = rn.nextInt();
			o.field37 = UUID.randomUUID().toString();
			o.field38 = rn.nextInt();
			o.field39 = UUID.randomUUID().toString();
			o.field40 = rn.nextInt();
			o.field41 = UUID.randomUUID().toString();
			o.field42 = rn.nextInt();
			o.field43 = UUID.randomUUID().toString();
			o.field44 = rn.nextInt();
			o.field45 = UUID.randomUUID().toString();
			o.field46 = rn.nextInt();
			o.field47 = UUID.randomUUID().toString();
			o.field48 = rn.nextInt();
			o.field49 = UUID.randomUUID().toString();
			o.field50 = rn.nextInt();
			o.field51 = UUID.randomUUID().toString();
			o.field52 = rn.nextInt();
			o.field53 = UUID.randomUUID().toString();
			o.field54 = rn.nextInt();
			o.field55 = UUID.randomUUID().toString();
			o.field56 = rn.nextInt();
			o.field57 = UUID.randomUUID().toString();
			o.field58 = rn.nextInt();
			o.field59 = UUID.randomUUID().toString();
			o.field60 = rn.nextInt();
			o.field61 = UUID.randomUUID().toString();
			o.field62 = rn.nextInt();
			o.field63 = UUID.randomUUID().toString();
			o.field64 = rn.nextInt();
			return o;
		}
	}

	@Benchmark
	public void igniteRead(IgniteInstance igniteInstance, Blackhole bh) throws Throwable {
		Long key = Math.abs(igniteInstance.r.nextLong()) % SIZE;
		ValueObject o = igniteInstance.cache.get(key);
		bh.consume(o.field1);		
	}

	
	@Benchmark
	public void igniteReadMultipleValues(IgniteInstance igniteInstance, Blackhole bh) throws Throwable {
		for (int i = 0; i < MULTIPLE_READ; i++) {
			Long key = Math.abs(igniteInstance.r.nextLong()) % SIZE;
			ValueObject o = igniteInstance.cache.get(key);
			bh.consume(o.field1);
		}
	}

	@Benchmark
	public void igniteReadKeepBinary(IgniteInstance igniteInstance, Blackhole bh) throws Throwable {
		Long key = Math.abs(igniteInstance.r.nextLong()) % SIZE;
		BinaryObject o = igniteInstance.cacheAsBinary.get(key);
		bh.consume((Long) igniteInstance.field.value(o));
	}
	
	@Benchmark
	public void igniteReadKeepBinaryMultipleValues(IgniteInstance igniteInstance, Blackhole bh) throws Throwable {
		for (int i = 0; i < MULTIPLE_READ; i++) {
			Long key = Math.abs(igniteInstance.r.nextLong()) % SIZE;
			BinaryObject o = igniteInstance.cacheAsBinary.get(key);
			bh.consume((Long) igniteInstance.field.value(o));	
		}
	}

	@Benchmark
	public void igniteReadKeepBinaryAndReadUncommitted(IgniteInstance igniteInstance, Blackhole bh) throws Throwable {
		Long key = Math.abs(igniteInstance.r.nextLong()) % SIZE;
		BinaryObject o = igniteInstance.cacheAsBinaryAndReadUncommitted.get(key);
		bh.consume((Long) igniteInstance.field.value(o));
	}
	
	@Benchmark
	public void igniteReadKeepBinaryAndReadUncommittedMultipleValues(IgniteInstance igniteInstance, Blackhole bh) throws Throwable {
		for (int i = 0; i < MULTIPLE_READ; i++) {
			Long key = Math.abs(igniteInstance.r.nextLong()) % SIZE;
			BinaryObject o = igniteInstance.cacheAsBinaryAndReadUncommitted.get(key);
			bh.consume((Long) igniteInstance.field.value(o));
		}
	}
	


//	@Benchmark
//	public void concurrentHashMapRead(ConcurrentHaspMapInstance concurrentHaspMapInstance, Key key, Blackhole bh)
//			throws Throwable {
//		bh.consume(concurrentHaspMapInstance.map.get(key.value));
//	}

	public static void main(String[] args) {
		IgniteInstance instance = new IgniteInstance();
		instance.setup();
		long binaryTime = 0;
		long nativeTime = 0;

		Long key = Math.abs(instance.r.nextLong()) % SIZE;
		BinaryObject bo = instance.cacheAsBinary.get(key);
		bo = instance.cacheAsBinaryAndReadUncommitted.get(key);
		Long field1 = (Long) instance.field.value(bo);
		System.out.println(field1);

//		key =  Math.abs(instance.r.nextLong()) % SIZE;
//		IgniteCache<Long, ValueObject> cacheRU = instance.cache.withReadUncommitted();
//		IgniteCache<Long, BinaryObject> cacheBI = cacheRU.withKeepBinary();
//		bo = cacheBI.get(key);
//		field1 = (Long) instance.field.value(bo);
//		System.out.println(field1);
//		
//		for (int j = 0; j < 1000; j++) {
//			
//			start = System.nanoTime();
//			ValueObject vo = instance.cache.get(key);
//			String field2 = vo.field1;
//			nativeTime += System.nanoTime() - start;
//		}
//		System.out.println("native : " + (nativeTime) + " binary : " + (binaryTime) + " rate : " + (double) binaryTime / (double) nativeTime);

	}

}
