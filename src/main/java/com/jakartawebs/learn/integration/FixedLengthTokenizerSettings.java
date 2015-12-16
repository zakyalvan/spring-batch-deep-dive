package com.jakartawebs.learn.integration;

import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * This type used to externalize settings of {@link FixedLengthTokenizer}.
 * 
 * @author zakyalvan
 * @since 1.0
 */
public class FixedLengthTokenizerSettings implements InitializingBean {
	private String[] names;
	private Range[] columns;
	
	public String[] getNames() {
		return names;
	}
	public void setNames(String[] names) {
		this.names = names;
	}
	public Range[] getColumns() {
		return columns;
	}
	public void setColumns(Range[] columns) {
		this.columns = columns;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(names, "Column or field names array must not be null");
		Assert.notNull(columns, "Column or field ranges must not be null");
		Assert.isTrue(names.length == columns.length, "Colum or field names and ranges must have same size");
	}
}