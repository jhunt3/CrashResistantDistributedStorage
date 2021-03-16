package shared.messages;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		SERVER_STOPPED, /* Server is stopped/initializing - no requests being processed */
		SERVER_WRITE_LOCK, /* Server locked for write due to reallocation of data - only get possible */
		SERVER_NOT_RESPONSIBLE, /* Server not responsible for key - need to update the metadata */

		// Admin Commands
		// Atomic Commands
		INIT_SERVER,
		INIT_SERVER_SUCCESS,
		INIT_SERVER_FAILED,
		START,
		START_SUCCESS,
//		START_FAILED,
		STOP,
		STOP_SUCCESS,
//		STOP_FAILED,
		SHUTDOWN,
		SHUTDOWN_SUCCESS,
//		SHUTDOWN_FAILED,
		LOCK,
		LOCK_SUCCESS,
//		LOCK_FAILED,
		UNLOCK,
		UNLOCK_SUCCESS,
//		UNLOCK_FAILED,
		MOVE_DATA,
		MOVE_DATA_SUCCESS,
		MOVE_DATA_FAILED,
		UPDATE,
		UPDATE_SUCCESS,
		UPDATE_FAILED,
		FLUSH,
		FLUSH_SUCCESS,
		FLUSH_FAILED,

		// Additional Replication Commands
		PROPAGATE,
	}


	public StatusType getStatus();

	/**
	 * @return the metadata map that maps the server ip:port (string) to the hash range it is
	 * responsible for (represented as a string range_start:range_end)
	 */
	public List<HashMap<String,String>> getMetadata();

	/**
	 *
	 * @return whether or not this message is from ECS or client
	 */
	public boolean isAdminMessage();
}


