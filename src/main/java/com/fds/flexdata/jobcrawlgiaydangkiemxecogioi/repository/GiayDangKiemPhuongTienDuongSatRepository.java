package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class GiayDangKiemPhuongTienDuongSatRepository {

    private final JdbcTemplate jdbcTemplate;

    public GiayDangKiemPhuongTienDuongSatRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> findDatas(String lastId, int batchSize) {
        String sql = """
                SELECT TOP (?) *
                FROM (
                    SELECT
                        CONCAT('TOA_XE|', gdk.SoGCN) AS __CHECKPOINT_ID,
                        gdk.SoGCN AS MaDinhDanh,
                        gdk.SoGCN AS SoGiay,
                        NULL AS SoTemKiemDinh,
                        gdk.NgayCapGCN AS NgayCap,
                        gdk.ThoiHanGCN AS NgayHetHan,
                        gdk.SoQLPT AS SoDangKiem,
                        gdk.NgayKT AS NgayDangKiem,
                        gdk.SoBBKT AS BaoCaoKiemTra,
                        gdk.NgayKT AS NgayBaoCao,
                        gdk.TrangThaiGCN AS 'TinhTrangHieuLucGiayTo.MaMuc',
                        COALESCE(NULLIF(slm.MaDinhDanh, ''), pt.SoQLPT) AS 'PhuongTien.MaDinhDanh',
                        pt.LoaiPhuongTien AS 'PhuongTien.LoaiPhuongTienDuongSat.MaMuc',
                        pt.SoQLPT AS 'PhuongTien.SoHieu',
                        pt.SoDangKy AS 'PhuongTien.GiayDangKyPhuongTienDuongSat',
                        pt.MaNuoc AS 'PhuongTien.NuocSanXuat.MaMuc',
                        pt.NuocSX AS 'PhuongTien.NuocSanXuat.TenMuc',
                        pt.NamSX AS 'PhuongTien.NamSanXuat',
                        pt.KhoDuong AS 'PhuongTien.KhoDuongSat.MaMuc',
                        pt.KieuLoaiToaXe AS 'PhuongTien.LoaiToaXe',
                        pt.ChieuDai AS 'PhuongTien.ChieuDai',
                        pt.ChieuRong AS 'PhuongTien.ChieuRong',
                        pt.ChieuCao AS 'PhuongTien.ChieuCao',
                        pt.LoaiGiaChuyenHuong AS 'PhuongTien.GiaChuyenHuong.MaMuc',
                        pt.LoaiVanHam AS 'PhuongTien.LoaiVanHam',
                        pt.LoaiMocNoi AS 'PhuongTien.LoaiMocNoi',
                        pt.TocDo AS 'PhuongTien.TocDoCauTao',
                        pt.SoChoNgoi AS 'PhuongTien.SoChoNgoi',
                        pt.SoChoNam AS 'PhuongTien.SoChoNam',
                        pt.TrongTai AS 'PhuongTien.TrongTai',
                        pt.KhoiLuong AS 'PhuongTien.KhoiLuongToaXe',
                        NULL AS 'PhuongTien.PhuongTienChuyenDung',
                        NULL AS 'PhuongTien.KyHieuDongCo',
                        NULL AS 'PhuongTien.SoDongCo',
                        NULL AS 'PhuongTien.CongThucTruc',
                        NULL AS 'PhuongTien.CongSuat',
                        NULL AS 'PhuongTien.DonViCongSuat'
                    FROM dbo.DS_GCNToaXe gdk
                    INNER JOIN dbo.DS_PTToaXe pt ON pt.SoQLPT = gdk.SoQLPT
                    LEFT JOIN dbo.DS_SaiLenhMaDinhDanh slm ON slm.SoQLPT = pt.SoQLPT

                    UNION ALL

                    SELECT
                        CONCAT('TOA_XE_DT|', gdk.SoGCN) AS __CHECKPOINT_ID,
                        gdk.SoGCN AS MaDinhDanh,
                        gdk.SoGCN AS SoGiay,
                        NULL AS SoTemKiemDinh,
                        gdk.NgayCapGCN AS NgayCap,
                        gdk.ThoiHanGCN AS NgayHetHan,
                        gdk.SoQLPT AS SoDangKiem,
                        gdk.NgayKT AS NgayDangKiem,
                        gdk.SoBBKT AS BaoCaoKiemTra,
                        gdk.NgayKT AS NgayBaoCao,
                        gdk.TrangThaiGCN AS 'TinhTrangHieuLucGiayTo.MaMuc',
                        COALESCE(NULLIF(slm.MaDinhDanh, ''), pt.SoQLPT) AS 'PhuongTien.MaDinhDanh',
                        pt.LoaiPhuongTien AS 'PhuongTien.LoaiPhuongTienDuongSat.MaMuc',
                        pt.SoQLPT AS 'PhuongTien.SoHieu',
                        pt.SoDangKy AS 'PhuongTien.GiayDangKyPhuongTienDuongSat',
                        pt.MaNuoc AS 'PhuongTien.NuocSanXuat.MaMuc',
                        pt.NuocSX AS 'PhuongTien.NuocSanXuat.TenMuc',
                        pt.NamSX AS 'PhuongTien.NamSanXuat',
                        pt.KhoDuong AS 'PhuongTien.KhoDuongSat.MaMuc',
                        NULL AS 'PhuongTien.LoaiToaXe',
                        pt.ChieuDai AS 'PhuongTien.ChieuDai',
                        pt.ChieuRong AS 'PhuongTien.ChieuRong',
                        pt.ChieuCao AS 'PhuongTien.ChieuCao',
                        NULL AS 'PhuongTien.GiaChuyenHuong.MaMuc',
                        NULL AS 'PhuongTien.LoaiVanHam',
                        NULL AS 'PhuongTien.LoaiMocNoi',
                        pt.TocDo AS 'PhuongTien.TocDoCauTao',
                        pt.SoCho AS 'PhuongTien.SoChoNgoi',
                        NULL AS 'PhuongTien.SoChoNam',
                        NULL AS 'PhuongTien.TrongTai',
                        pt.KhoiLuong AS 'PhuongTien.KhoiLuongToaXe',
                        pt.KieuLoaiTXDT AS 'PhuongTien.PhuongTienChuyenDung',
                        NULL AS 'PhuongTien.KyHieuDongCo',
                        NULL AS 'PhuongTien.SoDongCo',
                        NULL AS 'PhuongTien.CongThucTruc',
                        NULL AS 'PhuongTien.CongSuat',
                        NULL AS 'PhuongTien.DonViCongSuat'
                    FROM dbo.DS_GCNToaXeDT gdk
                    INNER JOIN dbo.DS_PTToaXeDT pt ON pt.SoQLPT = gdk.SoQLPT
                    LEFT JOIN dbo.DS_SaiLenhMaDinhDanh slm ON slm.SoQLPT = pt.SoQLPT

                    UNION ALL

                    SELECT
                        CONCAT('DAU_MAY|', gdk.SoGCN) AS __CHECKPOINT_ID,
                        gdk.SoGCN AS MaDinhDanh,
                        gdk.SoGCN AS SoGiay,
                        NULL AS SoTemKiemDinh,
                        gdk.NgayCapGCN AS NgayCap,
                        gdk.ThoiHanGCN AS NgayHetHan,
                        gdk.SoQLPT AS SoDangKiem,
                        gdk.NgayKT AS NgayDangKiem,
                        gdk.SoBBKT AS BaoCaoKiemTra,
                        gdk.NgayKT AS NgayBaoCao,
                        gdk.TrangThaiGCN AS 'TinhTrangHieuLucGiayTo.MaMuc',
                        COALESCE(NULLIF(slm.MaDinhDanh, ''), pt.SoQLPT) AS 'PhuongTien.MaDinhDanh',
                        pt.LoaiPhuongTien AS 'PhuongTien.LoaiPhuongTienDuongSat.MaMuc',
                        pt.SoQLPT AS 'PhuongTien.SoHieu',
                        pt.SoDangKy AS 'PhuongTien.GiayDangKyPhuongTienDuongSat',
                        pt.MaNuoc AS 'PhuongTien.NuocSanXuat.MaMuc',
                        pt.NuocSX AS 'PhuongTien.NuocSanXuat.TenMuc',
                        pt.NamSX AS 'PhuongTien.NamSanXuat',
                        pt.KhoDuong AS 'PhuongTien.KhoDuongSat.MaMuc',
                        NULL AS 'PhuongTien.LoaiToaXe',
                        pt.ChieuDai AS 'PhuongTien.ChieuDai',
                        pt.ChieuRong AS 'PhuongTien.ChieuRong',
                        pt.ChieuCao AS 'PhuongTien.ChieuCao',
                        NULL AS 'PhuongTien.GiaChuyenHuong.MaMuc',
                        NULL AS 'PhuongTien.LoaiVanHam',
                        NULL AS 'PhuongTien.LoaiMocNoi',
                        pt.TocDo AS 'PhuongTien.TocDoCauTao',
                        pt.SoCho AS 'PhuongTien.SoChoNgoi',
                        NULL AS 'PhuongTien.SoChoNam',
                        NULL AS 'PhuongTien.TrongTai',
                        NULL AS 'PhuongTien.KhoiLuongToaXe',
                        NULL AS 'PhuongTien.PhuongTienChuyenDung',
                        pt.LoaiDongCo AS 'PhuongTien.KyHieuDongCo',
                        pt.SoDongCo AS 'PhuongTien.SoDongCo',
                        pt.CongThucTruc AS 'PhuongTien.CongThucTruc',
                        pt.CongSuat AS 'PhuongTien.CongSuat',
                        pt.DonViCS AS 'PhuongTien.DonViCongSuat'
                    FROM dbo.DS_GCNDauMay gdk
                    INNER JOIN dbo.DS_PTDauMay pt ON pt.SoQLPT = gdk.SoQLPT
                    LEFT JOIN dbo.DS_SaiLenhMaDinhDanh slm ON slm.SoQLPT = pt.SoQLPT
                ) data
                WHERE (? = '' OR data.__CHECKPOINT_ID > ?)
                ORDER BY data.__CHECKPOINT_ID
                """;

        return jdbcTemplate.queryForList(sql, batchSize, lastId, lastId);
    }
}
