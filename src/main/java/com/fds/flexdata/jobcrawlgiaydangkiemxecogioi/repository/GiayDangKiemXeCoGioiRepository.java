package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Repository
public class GiayDangKiemXeCoGioiRepository {

    private final JdbcTemplate jdbcTemplate;

    public GiayDangKiemXeCoGioiRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> findDatas(LocalDateTime lastTime, String lastId, int size) {
        String sql = """
                 SELECT TOP (?)
                                              gcn.ID_GCN AS [Id],
                                              gcn.CreatedTime AS [SyncTime],
                                              gcn.SoTemKD AS [SoGiay],
                                              CONCAT(gcn.DonViKD, '-', gcn.SoPhieuKD) AS [MaDinhDanh],
                                              gcn.NgayCapGCN AS [NgayCap],
                                              gcn.DonViKD AS [NoiCap.MaDinhDanh],
                                              dv.TenDonVi AS [NoiCap.TenDinhDanh],
      
                                              gcn.ThoiHanGCN AS [NgayHetHan],
                                              gcn.NgayKD AS [NgayDangKiem],
                                              gcn.SoPhieuKD AS [SoPhieuKiemDinh],
                                              gcn.TrangThaiGCN AS [TinhTrangHieuLucGiayTo.MaMuc],
                                              gcn.KDVT AS [PhuongTien.KinhDoanhVanTai],
                                              gcn.Camera AS [PhuongTien.ThietBiGiamSatHinhAnh],
                                              gcn.TB_GSHT AS [PhuongTien.ThietBiGiamSatHanhTrinh],
                                              gcn.QR_URL AS [QR_URL],
                                              gcn.MucPhatThai AS [MucPhatThai],
        
                                              pt.SoKhung AS [PhuongTien.MaDinhDanh],
                                              pt.SoChoNgoi AS [PhuongTien.SoChoNgoi],
                                              pt.SoChoNam AS [PhuongTien.SoChoNam],
                                              pt.SoChoDung AS [PhuongTien.SoChoDung],
                                              pt.CongSuat AS [PhuongTien.CongSuat],
                                              pt.SoQuanLy AS [PhuongTien.SoQuanLy],
                                              pt.TuTrongTK AS [PhuongTien.KhoiLuongbanThan],
                                              pt.TaiTrongTK AS [PhuongTien.KhoiLuongHangTK],
                                              pt.TaiTrongGT AS [PhuongTien.KhoiLuongHangCP],
                                              pt.TrLgToanBoTK AS [PhuongTien.KhoiLuongToanBoTK],
                                              pt.TrLgToanBoGT AS [PhuongTien.KhoiLuongToanBoCP],
                                              pt.TrLgKeoTheoGT AS [PhuongTien.KhoiLuongKeoTheoCP],
                                              pt.KichThuocBao AS [PhuongTien.KichThuocBao],
                                              pt.KichThuocThung AS [PhuongTien.KichThuocLongThung],
                                              pt.BienDK AS [PhuongTien.BienSoXe],
                                              pt.LoaiPT AS [PhuongTien.PhanLoaiXeCoGioi.MaMuc],
                                              pt.SoMay AS [PhuongTien.SoMay],
                                              pt.SoKhung AS [PhuongTien.SoKhung],
                                              pt.NhanHieu AS [PhuongTien.NhanHieu],
                                              pt.NienHan AS [PhuongTien.NienHanSuDung],
                                              pt.MaKieuLoai AS [PhuongTien.SoLoai],
                                              pt.TenThuongMai AS [PhuongTien.TenThuongMai], 
                                              pt.MaNuoc AS [PhuongTien.NuocSanXuat.MaMuc],
                                              pt.NamSX AS [PhuongTien.NamSanXuat],
                                              pt.CaiTao AS [PhuongTien.XeDaCaiTao],
                                              pt.LoaiNhienLieu AS [PhuongTien.LoaiNhienLieu.MaMuc],
                                              pt.LoaiDongCo AS [PhuongTien.LoaiDongCo.MaMuc],
                                              pt.DungTich AS [PhuongTien.DungTich],
                                              pt.Hybrid AS [PhuongTien.Hybrid]
                                          FROM dbo.XCG_GiayCN gcn
                                          INNER JOIN dbo.XCG_PhuongTien pt
                                              ON gcn.ID_PT = pt.ID_PT
                                  				LEFT JOIN dbo.DM_DonViDK dv
                                  				ON gcn.DonViKD = dv.MaDV
                                          WHERE
                                              gcn.CreatedTime > ?
                                              OR (
                                                  gcn.CreatedTime = ?
                                                  AND gcn.ID_GCN > ?
                                              )
                                          ORDER BY
                                              gcn.CreatedTime ASC,
                                              gcn.ID_GCN ASC
        """;

        Timestamp lastTimestamp = Timestamp.valueOf(lastTime);

        return jdbcTemplate.queryForList(
                sql,
                size,
                lastTimestamp,
                lastTimestamp,
                lastId == null ? "" : lastId
        );
    }
}
