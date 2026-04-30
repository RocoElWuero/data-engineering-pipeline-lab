package com.oscar.salesapi.controller;

import com.oscar.salesapi.entity.SaleCleanSqlServer;
import com.oscar.salesapi.repository.SaleRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/sales")
public class SaleController {

	private final SaleRepository saleRepository;

	public SaleController(SaleRepository saleRepository) {
		this.saleRepository = saleRepository;
	}

	@GetMapping
	public List<SaleCleanSqlServer> getAllSales() {
		return saleRepository.findAll();
	}

	@GetMapping("/{id}")
	public ResponseEntity<SaleCleanSqlServer> getSaleById(@PathVariable Integer id) {
		return saleRepository.findById(id)
				.map(ResponseEntity::ok)
				.orElse(ResponseEntity.notFound().build());
	}

	@PostMapping
	public SaleCleanSqlServer saveSale(@RequestBody SaleCleanSqlServer sale) {
		return saleRepository.save(sale);
	}

	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deleteSale(@PathVariable Integer id) {
		if (!saleRepository.existsById(id)) {
			return ResponseEntity.notFound().build();
		}

		saleRepository.deleteById(id);
		return ResponseEntity.noContent().build();
	}
}
