package com.webperformance.muse.influx

import com.webperformance.muse.measurements.*
import org.influxdb.*
import org.influxdb.dto.*
import org.musetest.core.*
import org.musetest.core.events.*
import org.musetest.core.plugins.*
import org.musetest.core.step.descriptor.*
import org.musetest.core.steptest.*
import org.musetest.core.suite.*
import org.musetest.core.test.*
import org.slf4j.*
import java.util.concurrent.*

class MeasurementsToInfluxPlugin(val configuration: MeasurementsToInfluxConfiguration) : GenericConfigurablePlugin(configuration), MeasurementsConsumer
{
	private var hostname : String? = null
	private var port = 0
	private lateinit var client: InfluxDB
	private lateinit var ignore_meta: List<String>

	private lateinit var descriptors: StepDescriptors
	private val configs = mutableListOf<TestConfiguration>()
	private val step_names = mutableMapOf<Long, String>()
	
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
		var subject_type: String? = null
		var subject: String? = null
		val tags = mutableMapOf<String,String>()
		for (key in measurement.metadata.keys)
		{
			if (key.equals(Measurement.META_METRIC))
				metric = measurement.metadata[Measurement.META_METRIC].toString()
			else if (key.equals(Measurement.META_SUBJECT))
				subject = measurement.metadata[Measurement.META_SUBJECT].toString()
			else if (key.equals(Measurement.META_SUBJECT_TYPE))
				subject_type = measurement.metadata[Measurement.META_SUBJECT_TYPE].toString()
			else if (ignore_meta.contains(key))
				continue
			else
				tags[key] = measurement.metadata[key].toString()
		}
		
		if (metric == null)
		{
			LOG.error("metric not provided in measurement. Ignoring : " + measurement.toString())
			return
		}
		if (subject_type == null)
		{
			LOG.error("subject_type not provided in measurement. Ignoring : " + measurement.toString())
			return
		}
		
		if (subject != null)
			tags["name"] = lookupFriendlyName(subject, subject_type)
		
		val builder = Point.measurement(subject_type)
				.time(timestamp, TimeUnit.MILLISECONDS)
				.addField(metric, measurement.value)
				.tag(tags)
		val point = builder.build()
		client.write(point)
	}
	
	private fun lookupFriendlyName(subject: String, subject_type: String): String
	{
		if (subject_type == "step")
		{
			try
			{
				val step_id = subject.toLong()
				var name = step_names[step_id]
				if (name != null)
					return name
				else
				{
					for (config in configs)
					{
						val test = config.test() as SteppedTest
						val step = test.step.findByStepId(step_id)
						if (step != null)
						{
							name = descriptors.get(step).getShortDescription(step)
							if (name != null)
							{
								step_names[step_id] = name
								return name
							}
						}
					}
				}
			}
			catch (e: NumberFormatException)
			{
				// subject is not a stringified step-id
				return subject
			}
			
		}
		return subject
	}
	
	override fun initialize(context: MuseExecutionContext)
	{
		if (context is TestSuiteExecutionContext)
		{
			descriptors = context.getProject().getStepDescriptors()

			port = configuration.getPort(context)
			hostname = configuration.getHostname(context)
			if (hostname == null)
			{
				LOG.error("hostname parameter is required for MeasurementsToInfluxPlugin")
				context.raiseEvent(TestErrorEventType.create("hostname parameter is required for MeasurementsToInfluxPlugin"))
				return
			}
			ignore_meta = configuration.getIgnoreMetadata(context)
			
			val url = "http://$hostname:$port"
			client = InfluxDBFactory.connect(url)
			client.enableBatch(BatchOptions.DEFAULTS)
			client.setDatabase(configuration.getDatabase(context))
			
			// TODO register an exception handler for the client to report failures and create a new client
			
			context.addEventListener({ event ->
				if (EndSuiteEventType.TYPE_ID == event.typeId)
					client.close()
				if (StartSuiteTestEventType.TYPE_ID == event.typeId)
				{
					// store the test - used to lookup step names later
					val config = context.getVariable(StartSuiteTestEventType.getConfigVariableName(event))
					if (config is TestConfiguration)
						configs.add(config)
				}
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