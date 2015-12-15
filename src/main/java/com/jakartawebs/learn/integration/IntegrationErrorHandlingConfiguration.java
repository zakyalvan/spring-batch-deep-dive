package com.jakartawebs.learn.integration;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.mail.MailHeaders;
import org.springframework.integration.mail.MailSendingMessageHandler;
import org.springframework.mail.MailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;

@Configuration
@IntegrationComponentScan
public class IntegrationErrorHandlingConfiguration {
	@Autowired
	private JavaMailSender mailSender;
	
	@Bean
	public MailSendingMessageHandler mailSendingMessageHandler() {
		return new MailSendingMessageHandler(mailSender);
	}
	
	/**
	 * Please note, error channel will only catch error occurred on asynchronous flow,
	 * i.e. flow handled outside caller thread.
	 */
	@Bean
	public IntegrationFlow errorNotificationFlow(@Qualifier(MessageHeaders.ERROR_CHANNEL) MessageChannel errorChannel) {
		return IntegrationFlows.from(errorChannel)
				.route(payload -> MailMessage.class.isAssignableFrom(payload.getClass()), mapping -> mapping.subFlowMapping("true", sendMailFlow()).subFlowMapping("false", enrichAndSendMailFlow()))
				.get();
	}

	@Bean
	public IntegrationFlow sendMailFlow() {
		return flow -> flow.handle(mailSendingMessageHandler());
	}
	
	@Bean
	public IntegrationFlow enrichAndSendMailFlow() {
		return flow -> flow
				.enrichHeaders(headerSpec -> headerSpec.header(MailHeaders.TO, "zakyalvan@gmail.com").header(MailHeaders.SUBJECT, "Error Occured").header(MailHeaders.FROM, "lamonepoda@gmail.com"))
				.<Throwable, String>transform(throwable -> Arrays.asList(throwable.getStackTrace()).toString())
				.handle(mailSendingMessageHandler());
	}
}
