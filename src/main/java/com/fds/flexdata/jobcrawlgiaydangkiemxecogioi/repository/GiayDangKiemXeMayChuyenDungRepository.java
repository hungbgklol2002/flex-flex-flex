package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Repository
public class GiayDangKiemXeMayChuyenDungRepository {

    private static final String ID_PT_FIELD = "__ID_PT";
    private static final String THONG_SO_KY_THUAT_FIELD = "__THONG_SO_KY_THUAT_DAC_TRUNG";

    private final JdbcTemplate jdbcTemplate;

    public GiayDangKiemXeMayChuyenDungRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> findDatas(
            String lastId,
            int size
    ) {
        String sql = """
                SELECT TOP (?)
                  gcn.ID_GCN AS "__CHECKPOINT_ID",
                  pt.ID_PT AS "__ID_PT",
                  gcn.ID_GCN AS "MaDinhDanh",
                  gcn.DonViDK AS "NoiCap.MaDinhDanh",
                  dv.TenDonVi AS "NoiCap.TenDinhDanh",
                  gcn.SoBBKT AS "SoBienBanKiemTra",
                  gcn.NgayKT AS "ThoiGianKiemTra",
                  gcn.NoiKT AS "DiaChiKiemTra",
                  gcn.NgayCapGCN AS "NgayCap",
                  gcn.ThoiHanGCN AS "NgayHetHan",
                  gcn.ID_GCN AS "SoGiay",
                  gcn.TrangThaiGCN AS "TinhTrangHieuLucGiayTo.MaMuc",
                  pt.SoKhung AS "PhuongTien.MaDinhDanh",
                  pt.MaQL AS "PhuongTien.SoQuanLy",
                  pt.NhomXMCD AS "PhuongTien.NhomPhuongTienXeMayChuyenDung.MaMuc",
                  pt.LoaiXMCD AS "PhuongTien.LoaiPhuongTienXeMayChuyenDung.MaMuc",
                  pt.NhanHieu AS "PhuongTien.NhanHieu",
                  pt.TenThuongMai AS "PhuongTien.TenThuongMai",
                  pt.MaKieuLoai AS "PhuongTien.MaKieuLoai",
                  pt.SoKhung AS "PhuongTien.SoKhung",
                  pt.SoDongCo AS "PhuongTien.SoDongCo",
                  pt.MaNuocSX AS "PhuongTien.NuocSanXuat.MaMuc",
                  pt.TenNuocSX AS "PhuongTien.NuocSanXuat.TenMuc",
                  pt.NamSX AS "PhuongTien.NamSanXuat",
                  pt.KLBanThan AS "PhuongTien.KhoiLuongBanThan",
                  pt.ChieuDai AS "PhuongTien.ChieuDai",
                  pt.ChieuRong AS "PhuongTien.ChieuRong",
                  pt.ChieuCao AS "PhuongTien.ChieuCao",
                  pt.LoaiDongCo AS "PhuongTien.LoaiDongCo",
                  pt.KyHieuDongCo AS "PhuongTien.KyHieuDongCo",
                  pt.LoaiNhienLieu AS "PhuongTien.LoaiNhienLieu.MaMuc",
                  pt.CongSuat AS "PhuongTien.CongSuat",
                  pt.VongQuay AS "PhuongTien.TocDoQuay",
                  pt.VanTocDiChuyen AS "PhuongTien.VanTocDiChuyen"
                FROM dbo.XMCD_GiayCN gcn
                INNER JOIN dbo.XMCD_PhuongTien pt
                  ON pt.ID_PT = gcn.ID_PT
                LEFT JOIN dbo.DM_DonViDK dv
                  ON gcn.DonViDK = dv.MaDV
                WHERE (? = '' OR gcn.ID_GCN > ?)
                ORDER BY gcn.ID_GCN ASC
                """;

        List<Map<String, Object>> records = jdbcTemplate.queryForList(
                sql,
                size,
                lastId,
                lastId
        );

        attachThongSoKyThuat(records);
        return records;
    }

    private void attachThongSoKyThuat(List<Map<String, Object>> records) {
        List<String> idPts = records.stream()
                .map(record -> record.get(ID_PT_FIELD))
                .filter(Objects::nonNull)
                .map(Object::toString)
                .distinct()
                .toList();

        if (idPts.isEmpty()) {
            return;
        }

        String placeholders = String.join(",", Collections.nCopies(idPts.size(), "?"));
        String sql = """
                SELECT
                  ID_PT AS "__ID_PT",
                  ID_TS AS "MaThongSo",
                  TenThongSo AS "TenThongSo",
                  GiaTri AS "GiaTriThongSo",
                  DonViDo AS "DonViDo"
                FROM dbo.XMCD_ThongSoDT
                WHERE ID_PT IN (%s)
                """.formatted(placeholders);

        Map<String, List<Map<String, Object>>> thongSoByIdPt = jdbcTemplate
                .queryForList(sql, idPts.toArray())
                .stream()
                .collect(Collectors.groupingBy(
                        row -> Objects.toString(row.get(ID_PT_FIELD), ""),
                        LinkedHashMap::new,
                        Collectors.toList()
                ));

        for (Map<String, Object> record : records) {
            String idPt = Objects.toString(record.get(ID_PT_FIELD), "");
            record.put(THONG_SO_KY_THUAT_FIELD, thongSoByIdPt.getOrDefault(idPt, List.of()));
            record.remove(ID_PT_FIELD);
        }
    }
}
