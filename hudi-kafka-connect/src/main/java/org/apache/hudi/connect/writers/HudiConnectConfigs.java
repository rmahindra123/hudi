package org.apache.hudi.connect.writers;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.schema.FilebasedSchemaProvider;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Properties;

/**
 * Class storing configs for the HoodieWriteClient.
 */
@Immutable
@ConfigClassProperty(name = "Kafka Sink Connect Configurations",
    groupName = ConfigGroups.Names.KAFKA_CONNECT,
    description = "Configurations for Kakfa Connect Sink Connector for Hudi.")
public class HudiConnectConfigs extends HoodieConfig {

  private static final String KAFKA_VALUE_CONVERTER = "value.converter";

  public static final ConfigProperty<String> SCHEMA_PROVIDER_CLASS = ConfigProperty
      .key("hoodie.schemaprovider.schema.class")
      .defaultValue(FilebasedSchemaProvider.class.getName())
      .withDocumentation("subclass of org.apache.hudi.schema.SchemaProvider "
          + "to attach schemas to input & target table data, built in options: "
          + "org.apache.hudi.schema.FilebasedSchemaProvider.");

  public static final ConfigProperty<String> CONTROL_TOPIC_NAME = ConfigProperty
      .key("hoodie.kafka.control.topic")
      .defaultValue("hudi-control-topic")
      .withDocumentation("Kafka topic name used by the Hudi Sink Connector for "
          + "sending and receiving control messages. Not used for data records.");

  private HoodieWriteConfig hoodieWriteConfig;

  protected HudiConnectConfigs() {
    super();
  }

  protected HudiConnectConfigs(Properties props) {
    super(props);
    Properties newProps = new Properties();
    newProps.putAll(props);
    this.hoodieWriteConfig = HoodieWriteConfig.newBuilder().withProperties(newProps).build();
  }

  public static HudiConnectConfigs.Builder newBuilder() {
    return new HudiConnectConfigs.Builder();
  }

  public HoodieWriteConfig getHoodieWriteConfig() {
    return hoodieWriteConfig;
  }

  public String getSchemaProviderClass() {
    return getString(SCHEMA_PROVIDER_CLASS);
  }

  public String getControlTopicName() {
    return getString(CONTROL_TOPIC_NAME);
  }

  public String getKafkaValueConverter() {
    return getString(KAFKA_VALUE_CONVERTER);
  }

  public static class Builder {

    protected final HudiConnectConfigs connectConfigs = new HudiConnectConfigs();
    private boolean isWriteConfigSet = false;

    public Builder withControlTopicName(String controlTopicName) {
      connectConfigs.setValue(CONTROL_TOPIC_NAME, controlTopicName);
      return this;
    }

    public Builder withWriteConfig(HoodieWriteConfig writeConfig) {
      connectConfigs.getProps().putAll(writeConfig.getProps());
      isWriteConfigSet = true;
      return this;
    }

    // Kafka connect task are passed with props with type Map<>
    public Builder withProperties(Map<?, ?> properties) {
      connectConfigs.getProps().putAll(properties);
      return this;
    }

    public Builder withProperties(Properties properties) {
      connectConfigs.getProps().putAll(properties);
      return this;
    }

    protected void setDefaults() {
      // Check for mandatory properties
      connectConfigs.setDefaults(HudiConnectConfigs.class.getName());
      // Make sure the props is propagated
      connectConfigs.setDefaultOnCondition(
          !isWriteConfigSet, HoodieWriteConfig.newBuilder().withProperties(
              connectConfigs.getProps()).build());
    }

    public HudiConnectConfigs build() {
      setDefaults();
      //validate();
      // Build HudiConnectConfigs at the end
      return new HudiConnectConfigs(connectConfigs.getProps());
    }
  }
}
