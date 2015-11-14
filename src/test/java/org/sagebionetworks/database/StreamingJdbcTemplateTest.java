package org.sagebionetworks.database;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

public class StreamingJdbcTemplateTest {
	
	DataSource mockDatasource;
	Statement mockStatement;
	
	@Before
	public void before(){
		mockDatasource = Mockito.mock(DataSource.class);
		mockStatement = Mockito.mock(Statement.class);
	}
	
	@Test
	public void testApplyStatementSettings() throws SQLException{
		StreamingJdbcTemplate template = new StreamingJdbcTemplate(mockDatasource);
		assertEquals(Integer.MIN_VALUE, template.getFetchSize());
		// the Integer.MIN_VALUE  must be applied to a statement
		template.applyStatementSettings(mockStatement);
		verify(mockStatement).setFetchSize(Integer.MIN_VALUE);
	}

}
