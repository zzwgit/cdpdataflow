package org.apache.flink.table.client.cli;

import io.infinivision.flink.client.LocalExecutorExtend;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * client.
 */
public class Client {

	private static final Logger LOG = LoggerFactory.getLogger(Client.class);

	private final LocalExecutorExtend executor;

	private final SessionContext context;

	public Client(SessionContext context, LocalExecutorExtend executor) {
		this.context = context;
		this.executor = executor;
	}

	public SessionContext getContext() {
		return context;
	}

	public Executor getExecutor() {
		return executor;
	}

	private Optional<SqlCommandCall> parseCommand(String line) {
		final Optional<SqlCommandCall> parsedLine = SqlCommandParser.parse(line);
		if (!parsedLine.isPresent()) {
			printError(CliStrings.MESSAGE_UNKNOWN_SQL);
		}
		return parsedLine;
	}

	public void callCommand(SqlCommandCall cmdCall) {
		switch (cmdCall.command) {
		case RESET:
			callReset();
			break;
		case SET:
			callSet(cmdCall);
			break;
		case SHOW_CATALOGS:
			callShowCatalogs();
			break;
		case SHOW_DATABASES:
			callShowDatabases();
			break;
		case SHOW_TABLES:
			callShowTables();
			break;
		case SHOW_FUNCTIONS:
			callShowFunctions();
			break;
		case USE:
			callUseDatabase(cmdCall);
			break;
		case DESCRIBE:
		case DESC:
			callDescribe(cmdCall);
			break;
		case EXPLAIN:
			callExplain(cmdCall);
			break;
		case SELECT:
			callSelect(cmdCall);
			break;
		case INSERT_INTO:
			callInsertInto(cmdCall);
			break;
		case CREATE_TABLE:
			callCreateTable(cmdCall);
			break;
		case CREATE_VIEW:
			callCreateView(cmdCall);
			break;
		case CREATE_FUNCTION:
			callCreateFunction(cmdCall);
			break;
		case COMMIT:
			callCommitJob(cmdCall);
			break;
		default:
			throw new SqlClientException("Unsupported command: " + cmdCall.command);
		}
	}

	private void callReset() {
		context.resetSessionProperties();
		printInfo(CliStrings.MESSAGE_RESET);
	}

	private void callSet(SqlCommandCall cmdCall) {
		// show all properties
		if (cmdCall.operands.length == 0) {
			final Map<String, String> properties;
			try {
				properties = executor.getSessionProperties(context);
			} catch (SqlExecutionException e) {
				printExecutionException(e);
				return;
			}
		}
		// set a property
		else {
			context.setSessionProperty(cmdCall.operands[0], cmdCall.operands[1]);
		}
	}

	private void callShowCatalogs() {
		final List<String> catalogs;
		try {
			catalogs = executor.listCatalogs(context);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callShowDatabases() {
		final List<String> dbs;
		try {
			dbs = executor.listDatabases(context);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callShowTables() {
		final List<String> tables;
		try {
			tables = executor.listTables(context);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callShowFunctions() {
		final List<String> functions;
		try {
			functions = executor.listUserDefinedFunctions(context);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callUseDatabase(SqlCommandCall cmdCall) {
		try {
			executor.setDefaultDatabase(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callDescribe(SqlCommandCall cmdCall) {
		final TableSchema schema;
		try {
			schema = executor.getTableSchema(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callExplain(SqlCommandCall cmdCall) {
		final String explanation;
		try {
			explanation = executor.explainStatement(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private void callSelect(SqlCommandCall cmdCall) {
		final ResultDescriptor resultDesc;
		try {
			resultDesc = executor.executeQuery(context, cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return;
		}
	}

	private boolean callInsertInto(SqlCommandCall cmdCall) {

		try {
			final ProgramTargetDescriptor programTarget = executor.executeUpdate(context, cmdCall.operands[0]);
			printInfo("InsertInto has been created.");
		} catch (SqlExecutionException e) {
			printExecutionException(e);
			return false;
		}
		return true;
	}

	private void callCreateTable(SqlCommandCall cmdCall) {
		try {
			executor.createTable(context, cmdCall.operands[0]);
			printInfo(CliStrings.MESSAGE_TABLE_CREATE);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	private void callCreateView(SqlCommandCall cmdCall) {
		try {
			executor.createView(context, cmdCall.operands[0]);
			printInfo(CliStrings.MESSAGE_VIEW_CREATED);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	private void callCreateFunction(SqlCommandCall cmdCall) {
		try {
			executor.createFunction(context, cmdCall.operands[0]);
			printInfo(CliStrings.MESSAGE_FUNCTION_CREATE);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	private void callDropView(SqlCommandCall cmdCall) {
		final String name = cmdCall.operands[0];
		final ViewEntry view = context.getViews().get(name);

		if (view == null) {
			printExecutionError(CliStrings.MESSAGE_VIEW_NOT_FOUND);
			return;
		}

		try {
			// perform and validate change
			context.removeView(name);
			executor.validateSession(context);
			printInfo(CliStrings.MESSAGE_VIEW_REMOVED);
		} catch (SqlExecutionException e) {
			// rollback change
			context.addView(view);
			printExecutionException(CliStrings.MESSAGE_VIEW_NOT_REMOVED, e);
		}
	}

	private void callCommitJob(SqlCommandCall cmdCall) {
		try {
			executor.commitJob(context, cmdCall.operands[0]);
			printInfo(CliStrings.MESSAGE_VIEW_CREATED);
		} catch (SqlExecutionException e) {
			printExecutionException(e);
		}
	}

	// --------------------------------------------------------------------------------------------

	private void printExecutionException(Throwable t) {
		printExecutionException(null, t);
	}

	private void printExecutionException(String message, Throwable t) {
		final String finalMessage;
		if (message == null) {
			finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
		} else {
			finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR + ' ' + message;
		}
		printException(finalMessage, t);
	}

	private void printExecutionError(String message) {
		System.out.println(message);
	}

	private void printException(String message, Throwable t) {
		LOG.warn(message, t);
	}

	private void printError(String message) {
		System.out.println(message);
	}

	private void printInfo(String message) {
		System.out.println(message);
	}
}
