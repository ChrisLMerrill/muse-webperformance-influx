package com.webperformance.muse.influx

import com.webperformance.muse.measurements.*
import org.musetest.builtins.value.*
import org.musetest.core.*
import org.musetest.core.plugins.*
import org.musetest.core.resource.generic.*
import org.musetest.core.resource.types.*
import org.musetest.core.values.*
import org.musetest.core.values.descriptor.*

@MuseTypeId("com.webperformance.measurements-to-influx")
@MuseSubsourceDescriptors(
	MuseSubsourceDescriptor(displayName = "Apply automatically?", description = "If this source resolves to true, this plugin configuration will be automatically applied to tests", type = SubsourceDescriptor.Type.Named, name = GenericConfigurablePlugin.AUTO_APPLY_PARAM),
	MuseSubsourceDescriptor(displayName = "Apply only if", description = "Apply only if this source this source resolves to true", type = SubsourceDescriptor.Type.Named, name = GenericConfigurablePlugin.APPLY_CONDITION_PARAM),
	MuseSubsourceDescriptor(displayName = "Hostname", description = "Hostname of Influx server", type = SubsourceDescriptor.Type.Named, name = MeasurementsToInfluxConfiguration.HOSTNAME_PARAM),
	MuseSubsourceDescriptor(displayName = "Port", description = "Port on Influx server (defaults to 8086)", type = SubsourceDescriptor.Type.Named, name = MeasurementsToInfluxConfiguration.PORT_PARAM, optional = true),
	MuseSubsourceDescriptor(displayName = "Database", description = "Name of Influx database to send measurements to", type = SubsourceDescriptor.Type.Named, name = MeasurementsToInfluxConfiguration.DATABASE_PARAM, optional = true),
	MuseSubsourceDescriptor(displayName = "Ignore Metadata", description = "Measurement metadata attributes to ignore", type = SubsourceDescriptor.Type.List, name = MeasurementsToInfluxConfiguration.IGNORE_META_PARAM, optional = true)
)
class MeasurementsToInfluxConfiguration : GenericResourceConfiguration(), PluginConfiguration
{
	override fun getType(): ResourceType
	{
		return MeasurementsToInfluxType()
	}
	
	override fun createPlugin(): MeasurementsToInfluxPlugin
	{
		return MeasurementsToInfluxPlugin(this)
	}
	
	fun getHostname(context : MuseExecutionContext): String?
	{
		val hostname_source_config = parameters[HOSTNAME_PARAM]
		if (hostname_source_config == null)
			return null;
		
		val hostname_source = hostname_source_config.createSource()
		return BaseValueSource.getValue(hostname_source, context, false, String::class.java)
	}
	
	fun getPort(context : MuseExecutionContext): Int
	{
		var port = 8086

		val port_source_config = parameters[PORT_PARAM]
		if (port_source_config != null)
		{
			val port_source = port_source_config.createSource()
			val value = BaseValueSource.getValue(port_source, context, false, Number::class.java)
			if (value != null)
				port = value.toInt()
		}
		return port;
	}
	
	fun getDatabase(context : MuseExecutionContext): String
	{
		var database = "telegraf"

		val database_source_config = parameters[DATABASE_PARAM]
		if (database_source_config != null)
		{
			val port_source = database_source_config.createSource()
			val value = BaseValueSource.getValue(port_source, context, false, String::class.java)
			if (value != null)
				database = value
		}
		return database;
	}
	
	fun getIgnoreMetadata(context: MuseExecutionContext): List<String>
	{
		val meta_to_ignore = mutableListOf<String>()
		val ignore_config = parameters[IGNORE_META_PARAM]
		if (ignore_config != null)
		{
			val ignore_source = ignore_config.createSource(context.project)
			val value = BaseValueSource.getValue(ignore_source, context, true, List::class.java)
			if (value != null)
				for (ignore in value.listIterator())
					meta_to_ignore.add(ignore.toString())
		}
		
		return meta_to_ignore.toList()
	}
	
	class MeasurementsToInfluxType : ResourceSubtype(TYPE_ID, "Send Measurements to Influx", MeasurementsToInfluxConfiguration::class.java, PluginConfiguration.PluginConfigurationResourceType())
	{
		override fun create(): MuseResource
		{
			val config = MeasurementsToInfluxConfiguration()
			config.parameters().addSource(GenericConfigurablePlugin.AUTO_APPLY_PARAM, ValueSourceConfiguration.forValue(true))
			config.parameters().addSource(GenericConfigurablePlugin.APPLY_CONDITION_PARAM, ValueSourceConfiguration.forValue(true))
			val ignore_list = ValueSourceConfiguration.forType(ListSource.TYPE_ID)
			ignore_list.addSource(ValueSourceConfiguration.forValue(Measurement.META_SEQUENCE))
			ignore_list.addSource(ValueSourceConfiguration.forValue(Measurement.META_TIMESTAMP))
			config.parameters().addSource(IGNORE_META_PARAM, ignore_list)
			return config
		}
		
		override fun getDescriptor(): ResourceDescriptor
		{
			return DefaultResourceDescriptor(this, "Sends measurements to an InfluxDB server.")
		}
	}
	
	companion object
	{
		val TYPE_ID = MeasurementsToInfluxConfiguration::class.java.getAnnotation(MuseTypeId::class.java).value
		const val HOSTNAME_PARAM = "hostname"
		const val PORT_PARAM = "port"
		const val DATABASE_PARAM = "database"
		const val IGNORE_META_PARAM = "ignore_meta"
	}
}