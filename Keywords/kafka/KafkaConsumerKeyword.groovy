package kafka

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject

import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.checkpoint.Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling
import com.kms.katalon.core.testcase.TestCase
import com.kms.katalon.core.testdata.TestData
import com.kms.katalon.core.testobject.TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows

import internal.GlobalVariable

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Arrays
import java.util.Properties
import com.kms.katalon.core.annotation.Keyword

public class KafkaConsumerKeyword {
	
	@Keyword
	def consumeMessage(String topicName) {
		Properties props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")    // alamat Kafka broker
		props.put("group.id", "katalon-consumer-group")
		props.put("key.deserializer", StringDeserializer.class.getName())
		props.put("value.deserializer", StringDeserializer.class.getName())
		props.put("auto.offset.reset", "earliest")

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)
		consumer.subscribe(Arrays.asList(topicName))

		println "📡 Listening to topic: ${topicName}"

		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5))
		for (ConsumerRecord<String, String> record : records) {
			println "✅ Received message: ${record.value()}"
			assert record.value() != null
		}

		consumer.close()
	}
}
