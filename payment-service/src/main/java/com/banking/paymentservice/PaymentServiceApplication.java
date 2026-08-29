package com.banking.paymentservice;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PaymentServiceApplication {

	public static void main(String[] args) {
		// Load from current working directory or payment-service folder
		Dotenv dotenv = Dotenv.configure()
				.directory("./payment-service")
				.ignoreIfMissing()
				.load();
		dotenv.entries().forEach(entry -> System.setProperty(entry.getKey(), entry.getValue()));

		Dotenv rootDotenv = Dotenv.configure()
				.ignoreIfMissing()
				.load();
		rootDotenv.entries().forEach(entry -> System.setProperty(entry.getKey(), entry.getValue()));

		SpringApplication.run(PaymentServiceApplication.class, args);
	}

}

