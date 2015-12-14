package com.jakartawebs.learn.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {
	private static final Logger LOGGER = LoggerFactory.getLogger(PersonItemProcessor.class);
	
	@Override
	public Person process(Person item) throws Exception {
		Person transformedItem = new Person(item.getFirstName().toUpperCase(), item.getLastName().toUpperCase());
		LOGGER.debug("Transforming person from {} to {}", item, transformedItem);
		return transformedItem;
	}
}
