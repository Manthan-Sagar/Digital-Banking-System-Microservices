package com.banking.transactionservice.repository;

import com.banking.transactionservice.model.Transaction;
import com.banking.transactionservice.model.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {
    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);

}
