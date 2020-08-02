package mail;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class mailUtils {
	public static final String HOST_NAME = "smtp.gmail.com";

	public static final int SSL_PORT = 465; // Port for SSL

	public static final int TSL_PORT = 587; // Port for TLS/STARTTLS

	public static final String APP_EMAIL = "Tung0164851@gmail.com"; // your email

	public static final String APP_PASSWORD = "njooxzjotjnxqaru"; // your password

	public static void SendMail(String reciver, String subject, String text) {
		// Get properties object
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "465");
		props.put("mail.transport.protocol", "smtp");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(APP_EMAIL, APP_PASSWORD);
			}
		});

		try {
			Transport transport = session.getTransport();
			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(APP_EMAIL));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(reciver));
			message.setSubject(subject);// formBean.getString(
			message.setText(text);
			transport.connect();
			Transport.send(message, InternetAddress.parse(reciver));// (message);

		} catch (MessagingException e) {
			System.out.println("e=" + e);
			e.printStackTrace();
			throw new RuntimeException(e);

		}
	}

}
