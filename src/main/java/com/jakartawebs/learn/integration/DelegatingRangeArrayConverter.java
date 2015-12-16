package com.jakartawebs.learn.integration;

import java.beans.PropertyEditor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.convert.converter.Converter;

/**
 * Convert string to array of {@link Range} object, delegating to {@link RangeArrayPropertyEditor}.
 * This adapter required because no mechanism (at least I don't know) to register custom {@link PropertyEditor} 
 * for binding {@link ConfigurationProperties} bean properties.
 * 
 * @author zakyalvan
 * @since 1.0
 */
public class DelegatingRangeArrayConverter implements Converter<String, Range[]> {
	private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingRangeArrayConverter.class);

	private final RangeArrayPropertyEditor delegatePropertyEditor;

	public DelegatingRangeArrayConverter() {
		this.delegatePropertyEditor = new RangeArrayPropertyEditor();
	}
	public DelegatingRangeArrayConverter(RangeArrayPropertyEditor delegatePropertyEditor) {
		this.delegatePropertyEditor = delegatePropertyEditor;
	}
	
	public void setForceDisjointRanges(boolean forceDisjointRanges) {
		delegatePropertyEditor.setForceDisjointRanges(forceDisjointRanges);
	}

	@Override
	public Range[] convert(String source) {
		LOGGER.debug("Converting {} into range array object", source);
		delegatePropertyEditor.setAsText(source);
		Range[] result = (Range[]) delegatePropertyEditor.getValue();
		LOGGER.debug("Converted {} into {}", source, result);
		return result;
	}
}