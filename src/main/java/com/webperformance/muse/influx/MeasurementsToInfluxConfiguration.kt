package com.webperformance.muse.influx

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
	MuseSubsourceDescriptor(displayName = "Port", description = "Port on Influx server (defaults to 8086)", type = SubsourceDescriptor.Type.Named, name = MeasurementsToInfluxConfiguration.PORT_PARAM, optional = true)
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
		var port = 2003

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
	
	class MeasurementsToInfluxType : ResourceSubtype(TYPE_ID, "Send Measurements to Influx", MeasurementsToInfluxConfiguration::class.java, PluginConfiguration.PluginConfigurationResourceType())
	{
		override fun create(): MuseResource
		{
			val config = MeasurementsToInfluxConfiguration()
			config.parameters().addSource(GenericConfigurablePlugin.AUTO_APPLY_PARAM, ValueSourceConfiguration.forValue(true))
			config.parameters().addSource(GenericConfigurablePlugin.APPLY_CONDITION_PARAM, ValueSourceConfiguration.forValue(true))
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
	}
}