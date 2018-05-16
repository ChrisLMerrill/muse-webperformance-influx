package com.webperformance.muse.influx

import com.webperformance.muse.measurements.*
import org.influxdb.*
import org.influxdb.dto.*
import org.musetest.core.*
import org.musetest.core.events.*
import org.musetest.core.plugins.*
import org.musetest.core.suite.*
import org.slf4j.*
import java.util.concurrent.*

class MeasurementsToInfluxPlugin(val configuration: MeasurementsToInfluxConfiguration) : GenericConfigurablePlugin(configuration), MeasurementsConsumer
{
	private var hostname : String? = null
	private var port = 0
	private lateinit var client: InfluxDB
	
	override fun acceptMeasurements(measurements: Measurements)
	{
		for (measurement in measurements.iterator())
			writeMeasurement(measurement)
	}
	
	private fun writeMeasurement(measurement: Measurement)
	{
		val timestamp: Long
		if (measurement.metadata[Measurement.META_TIMESTAMP] is Long)
			timestamp = measurement.metadata[Measurement.META_TIMESTAMP] as Long
		else
			return

		var metric: String? = null
		val tags = mutableMapOf<String,String>()
		for (key in measurement.metadata.keys)
		{
			if (key.equals(Measurement.META_METRIC))
				metric = measurement.metadata[Measurement.META_METRIC].toString()
			else if (key.equals(Measurement.META_SEQUENCE)
				  || key.equals(Measurement.META_TIMESTAMP))
				continue
			else
				tags[key] = measurement.metadata[key].toString()
		}
		
		if (metric == null)
		{
			LOG.error("metric not provided in measurement. Ignoring : " + measurement.toString())
			return
		}
		
		val builder = Point.measurement(metric)
				.time(timestamp, TimeUnit.MILLISECONDS)
				.addField("value", measurement.value)
				.tag(tags)
		client.write(builder.build())
	}
	
	
	override fun initialize(context: MuseExecutionContext)
	{
		if (context is TestSuiteExecutionContext)
		{
			port = configuration.getPort(context)
			hostname = configuration.getHostname(context)
			if (hostname == null)
			{
				LOG.error("hostname parameter is required for MeasurementsToInfluxPlugin")
				context.raiseEvent(TestErrorEventType.create("hostname parameter is required for MeasurementsToInfluxPlugin"))
				return
			}
			
			val url = "http://$hostname:$port"
			client = InfluxDBFactory.connect(url)
			client.enableBatch(BatchOptions.DEFAULTS)
			client.setDatabase("telegraf")
			
			// TODO register an exception handler for the client to report failures and create a new client
			
			context.addEventListener({ event ->
				if (EndSuiteEventType.TYPE_ID == event.typeId)
					client.close()
			})
		}
	}
	
	override fun applyToContextType(context: MuseExecutionContext?): Boolean
	{
		return context is TestSuiteExecutionContext
	}
	
	companion object
	{
		val LOG = LoggerFactory.getLogger(MeasurementsToInfluxPlugin::class.java)
	}
}