package com.jakartawebs.learn.integration;

import java.io.Serializable;
import java.util.Date;

@SuppressWarnings("serial")
public class Person implements Serializable {
	private String firstName;
	private String lastName;
	private String emailAddress;
	private Date dateOfBirth;
	
	public Person() {}
	public Person(String firstName, String lastName, String emailAddress, Date dateOfBirth) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.emailAddress = emailAddress;
		this.dateOfBirth = dateOfBirth;
	}
	
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	public String getEmailAddress() {
		return emailAddress;
	}
	public void setEmailAddress(String emailAddress) {
		this.emailAddress = emailAddress;
	}
	
	public Date getDateOfBirth() {
		return dateOfBirth;
	}
	public void setDateOfBirth(Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}
	
	@Override
	public String toString() {
		return "Person [firstName=" + firstName + ", lastName=" + lastName + ", emailAddress=" + emailAddress
				+ ", dateOfBirth=" + dateOfBirth + "]";
	}
}
