/* 
	Purpose:
		
	Description:
		
	History:
		2014/11/27, Created by JerryChen

Copyright (C) 2013 Potix Corporation. All Rights Reserved.

*/
package org.zkoss.zss.app.ui.dlg;

import org.model.AutoRollbackConnection;
import org.model.DBContext;
import org.model.DBHandler;
import org.zkoss.lang.Strings;
import org.zkoss.zk.ui.Executions;
import org.zkoss.zk.ui.event.EventListener;
import org.zkoss.zk.ui.select.annotation.Listen;
import org.zkoss.zk.ui.select.annotation.Wire;
import org.zkoss.zul.Textbox;
import org.zkoss.zul.Window;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * 
 * @author JerryChen
 *
 */
public class UsernameCtrl extends DlgCtrlBase{
	public final static String ARG_NAME = "username";
    public final static String ARG_PASSWORD = "password";
    public final static String MESSAGE = "message";
    public static final String ON_USERNAME_CHANGE = "onUsernameChange";
    private static final long serialVersionUID = 1L;
    private final static String URI = "~./zssapp/dlg/username.zul";
    private final static String SALT = "DataSpread";
    @Wire
	Textbox username;
    @Wire
    Textbox password;

    public static void show(EventListener<DlgCallbackEvent> callback, String username, String message) {
        Map arg = newArg(callback);

        arg.put(ARG_NAME, username);
        arg.put(ARG_PASSWORD, "");
        arg.put(MESSAGE, message);

        Window comp = (Window) Executions.createComponents(URI, null, arg);
        comp.doModal();
        return;
    }
	
	@Override
	public void doAfterCompose(Window comp) throws Exception {
		super.doAfterCompose(comp);
		Map<?, ?> args = Executions.getCurrent().getArg();
		if(args.containsKey(ARG_NAME))
			username.setValue((String) args.get(ARG_NAME));
        if (args.containsKey(ARG_PASSWORD))
            password.setValue((String) args.get(ARG_PASSWORD));
        if (args.containsKey(MESSAGE))
            username.setErrorMessage((String) args.get(MESSAGE));
    }

	@Listen("onClick=#ok; onOK=#usernameDlg")
	public void onSave(){
		if(Strings.isBlank(username.getValue())){
			username.setErrorMessage("empty name is not allowed");
			return;
		}
        if (Strings.isBlank(password.getValue()) && (!username.getValue().equals("guest"))) {
            password.setErrorMessage("empty password is not allowed");
            return;
        }
        if (!username.getValue().equals("guest")) {
            try (AutoRollbackConnection connection = DBHandler.instance.getConnection()) {
                //get user info
                String select = "SELECT password FROM user_account WHERE username = ?;";
                PreparedStatement selectStmt = connection.prepareStatement(select);
                selectStmt.setString(1, username.getValue());
                ResultSet rs = selectStmt.executeQuery();
                if (rs.next()) {
                    String pswd = rs.getString(1);
                    String saltedPassword = SALT + password.getValue();
                    String hashedPassword = generateHash(saltedPassword);
                    if (!pswd.equals(hashedPassword)) {
                        password.setErrorMessage("Wrong Password");
                        return;
                    }
                } else {
                    username.setErrorMessage("Your username does not exist");
                    return;
                }
                selectStmt.close();
                connection.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

		postCallback(ON_USERNAME_CHANGE, newMap(newEntry(ARG_NAME, username.getValue())));
		detach();
	}

    @Listen("onClick=#cancel; onCancel=#usernameDlg")
    public void onCancel() {
        detach();
    }

    private String generateHash(String input) {
        StringBuilder hash = new StringBuilder();
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = sha.digest(input.getBytes());
            char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                    'a', 'b', 'c', 'd', 'e', 'f'};
            for (int idx = 0; idx < hashedBytes.length; ++idx) {
                byte b = hashedBytes[idx];
                hash.append(digits[(b & 0xf0) >> 4]);
                hash.append(digits[b & 0x0f]);
            }
        } catch (NoSuchAlgorithmException e) {
            // handle error here.
        }

        return hash.toString();
    }
}
