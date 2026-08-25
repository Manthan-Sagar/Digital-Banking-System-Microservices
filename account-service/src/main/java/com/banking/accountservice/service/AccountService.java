package com.banking.accountservice.service;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.model.Account;
import com.banking.accountservice.model.AccountStatus;
import com.banking.accountservice.model.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.banking.accountservice.exception.*;


import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.Map;
// import java.util.Random;

@Service
@RequiredArgsConstructor
@Slf4j
public class AccountService {

    private final AccountRepository accountRepository;
    private static SecureRandom secureRandom = new SecureRandom();


    @Transactional
    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating account for: {}", request.getEmail());

        if (accountRepository.existsByEmail(request.getEmail())) {
            throw new AccountAlreadyExistsException("Account already exists for email: "
                    + request.getEmail());
        }

        Account account = new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(
                request.getAccountType() == AccountType.SAVINGS
                        ? new BigDecimal("100000")
                        : new BigDecimal("500000")
        );

        Account saved = accountRepository.save(account);
        log.info("Account created: {}", saved.getAccountNumber());
        return mapToResponse(saved);
    }


    public AccountResponse getAccount(String accountNumber) {
        Account account = findByAccountNumber(accountNumber);
        return mapToResponse(account);
    }


    public BigDecimal getBalance(String accountNumber) {
        return findByAccountNumber(accountNumber).getBalance();
    }


    @Transactional
    public void blockAccount(String accountNumber) {
        log.info("Blocking account: {}", accountNumber);
        Account account = findByAccountNumber(accountNumber);
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked: {}", accountNumber);
    }


    @Transactional
    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting {} from account: {}", amount, accountNumber);
        Account account = findByAccountNumber(accountNumber);

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new AccountNotActiveException("Account is not active: " + accountNumber);
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new InsufficientBalanceException("Insufficient balance");
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance deducted. New balance: {}", account.getBalance());
    }


    @Transactional
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting {} to account: {}", amount, accountNumber);
        Account account = findByAccountNumber(accountNumber);
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance credited. New balance: {}", account.getBalance());
    }


    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {
        try {
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Crediting account: {} amount: {}", receiverAccount, amount);
            creditBalance(receiverAccount, amount);
        } catch (Exception e) {
            log.error("Error crediting account: {}", e.getMessage());
        }
    }


    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected — blocking account: {}", accountNumber);
            blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error blocking account: {}", e.getMessage());
        }
    }

    
    private String generateAccountNumber() {
        String accountNumber;

        do {
            long number = secureRandom.nextLong(1_000_000_000_000L);

            accountNumber = String.format("%012d", number);

        } while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private Account findByAccountNumber(String accountNumber) {
        return accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Account not found: " + accountNumber));
    }

    private AccountResponse mapToResponse(Account account) {
        AccountResponse response = new AccountResponse();
        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setStatus(account.getStatus());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());
        return response;
    }
}
