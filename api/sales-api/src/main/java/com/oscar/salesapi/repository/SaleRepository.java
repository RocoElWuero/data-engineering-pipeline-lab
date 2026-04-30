package com.oscar.salesapi.repository;

import com.oscar.salesapi.entity.SaleCleanSqlServer;
import org.springframework.data.jpa.repository.JpaRepository;

// Capa de acceso a datos (DAO automático)
public interface SaleRepository extends JpaRepository<SaleCleanSqlServer, Integer> {
}
