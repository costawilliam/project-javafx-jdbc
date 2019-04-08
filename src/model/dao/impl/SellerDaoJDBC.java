package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao {

	private Connection conn;

	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller seller) {
		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("" + "INSERT INTO seller(" 
					+ "name, email, birthdate, basesalary, departmentid)"
					+ "VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);

			st.setString(1, seller.getName());
			st.setString(2, seller.getEmail());
			st.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			st.setDouble(4, seller.getBaseSalary());
			st.setInt(5, seller.getDepartment().getId());

			int rowsAffected = st.executeUpdate();

			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					seller.setId(id);
				}
				DB.closeResultSet(rs);

			} else {
				throw new DbException("Unexpected error! No rows affedcted!");
			}
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller seller) {
		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("UPDATE seller "
					+ "SET name = ?, email = ?, birthdate = ?, basesalary = ?, departmentid = ? " 
					+ "WHERE ID = ?", Statement.RETURN_GENERATED_KEYS);

			st.setString(1, seller.getName());
			st.setString(2, seller.getEmail());
			st.setDate(3, new java.sql.Date(seller.getBirthDate().getTime()));
			st.setDouble(4, seller.getBaseSalary());
			st.setInt(5, seller.getDepartment().getId());
			st.setInt(6, seller.getId());

			st.executeUpdate();
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("DELETE FROM seller WHERE id = ?", Statement.RETURN_GENERATED_KEYS);
			st.setInt(1, id);
			st.executeUpdate();
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement("SELECT s.*, d.name AS DEPNAME FROM seller s "
					+ "JOIN department d ON d.id = s.departmentid WHERE s.id = ?");
			st.setInt(1, id);

			rs = st.executeQuery();

			if (rs.next()) {
				Department department = instantiateDepartment(rs);
				Seller seller = instantiateSeller(rs, department);

				return seller;
			} else {
				return null;
			}
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs, Department department) throws SQLException {
		Seller seller = new Seller();
		seller.setId(rs.getInt("ID"));
		seller.setName(rs.getString("NAME"));
		seller.setEmail(rs.getString("EMAIL"));
		seller.setBaseSalary(rs.getDouble("BASESALARY"));
		seller.setBirthDate(rs.getDate("BIRTHDATE"));
		seller.setDepartment(department);

		return seller;
	}

	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department department = new Department();
		department.setId(rs.getInt("DEPARTMENTID"));
		department.setName(rs.getString("DEPNAME"));

		return department;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement("SELECT s.*, d.name AS DEPNAME FROM seller s "
					+ "JOIN department d ON d.id = s.departmentid " 
					+ "ORDER BY s.name");

			rs = st.executeQuery();

			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();

			while (rs.next()) {
				Department department = map.get(rs.getInt("DEPARTMENTID"));

				if (department == null) {
					department = instantiateDepartment(rs);
					map.put(rs.getInt("DEPARTMENTID"), department);
				}

				Seller seller = instantiateSeller(rs, department);

				list.add(seller);
			}
			return list;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	@Override
	public List<Seller> findByDepartment(Department dep) {
		PreparedStatement st = null;
		ResultSet rs = null;

		try {
			st = conn.prepareStatement("SELECT s.*, d.name AS DEPNAME FROM seller s "
					+ "JOIN department d ON d.id = s.departmentid WHERE s.departmentid = ? " 
					+ "ORDER BY s.name");
			st.setInt(1, dep.getId());

			rs = st.executeQuery();

			List<Seller> list = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<>();

			while (rs.next()) {
				Department department = map.get(rs.getInt("DEPARTMENTID"));

				if (department == null) {
					department = instantiateDepartment(rs);
					map.put(rs.getInt("DEPARTMENTID"), department);
				}

				Seller seller = instantiateSeller(rs, department);

				list.add(seller);
			}
			return list;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

}
