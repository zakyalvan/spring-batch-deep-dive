package com.jakartawebs.learn.integration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/people")
public class PeopleController {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@RequestMapping(method=RequestMethod.GET)
	@Transactional(readOnly=true)
	public Collection<Person> listPeople() {
		return jdbcTemplate.query("SELECT first_name, last_name, email_address, date_of_birth FROM integration_people", new RowMapper<Person>() {
			@Override
			public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
				return new Person(rs.getString(1), rs.getString(2), rs.getString(3), rs.getDate(4));
			}
		});
	}
}
