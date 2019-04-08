package gui.util;

import java.awt.event.ActionEvent;

import javafx.scene.Node;
import javafx.stage.Stage;

public class Utils {
	public static Stage currentStage(javafx.event.ActionEvent event) {		
		return (Stage) ((Node) event.getSource()).getScene().getWindow();		
	}
}
