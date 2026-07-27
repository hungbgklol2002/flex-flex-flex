package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class GiayDangKyPhuongTienDTNDRepository {

    private final JdbcTemplate jdbcTemplate;

    public GiayDangKyPhuongTienDTNDRepository(
            JdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> findDatas(
            Timestamp lastNgayCap,
            String lastSoGiay,
            int batchSize
    ) {

        String sql = """
                SELECT TOP (%d)
                                                             tb1.SoGiay AS "__CHECKPOINT_SO_GIAY",
                                                             tb1.SoGiay as "MaDinhDanh",
                                                             tb1.SoGiay as "SoGiay",
                                                             tb1.NgayCapGCN as "NgayCap",
                                                             tb1.HanGCN as "NgayHetHan",
                                                             tb1.SoKiemSoat as "SoKiemSoat",
                                                             tb1.MaDonVi as "NoiCap.MaDinhDanh",
                                                             dv.TenDonVi as "NoiCap.TenToChuc",
                                                             tb1.SoBienBanKT as "SoPhieuKiemDinh",\s
                                                             tb2.SoKiemSoat as "PhuongTien.MaDinhDanh",
                                                             tb2.SoKiemSoat as "PhuongTien.SoKiemSoat",
                                                             tb2.CapTauCB as "PhuongTien.CapPhuongTienThuyNoiDia.MaMuc",
                                                             tb2.CongDung as "PhuongTien.CongDungPhuongTien",
                                                             tb2.DungTich as "PhuongTien.TongDungTich",
                                                             tb2.NamDong as "PhuongTien.NamDong",
                                                             tb2.NoiDong as "PhuongTien.NoiDong",
                                                             tb2.SoDangKyHanhChinh as "PhuongTien.SoDangKyHanhChinh",
                                                             tb2.SoLuongMayChinh as "PhuongTien.SoLuongMC",
                                                             tb2.TenPhuongTien as "PhuongTien.TenTau",
                                                             tb2.TongCongSuatMayChinh as "PhuongTien.CongSuatMayChinh",
                                                             tb2.TrongTaiToanPhan as "PhuongTien.TrongTaiToanPhan",
                                                             tb2.VatLieu as "PhuongTien.VatLieuThanTau",
                                                             tb2.SoDangKiem as "PhuongTien.SoDangKiem",
                                                             tb2.Lmax as "PhuongTien.ChieuDaiLonNhat",
                                                             tb2.L as "PhuongTien.ChieuDaiThietKe",
                                                             tb2.Bmax as "PhuongTien.ChieuRongLonNhat",
                                                             tb2.B as "PhuongTien.ChieuRongThietKe",
                                                             tb2.D as "PhuongTien.ChieuCaoMan",
                                                             tb2.NhomPhuongTien as "PhuongTien.NhomPhuongTienThuyNoiDia.MaMuc"
                
                                                           FROM [dbo].[TS_GiayChungNhan] tb1
                                                           LEFT JOIN [dbo].[TS_Tau] tb2
                                                             ON tb1.SoKiemSoat = tb2.SoKiemSoat
                                                           LEFT JOIN [dbo].[DMDonViDK_KhoiThuy] dv
                                                             ON tb1.MaDonVi = dv.MaDonVi
                                                           WHERE tb1.NgayCapGCN IS NOT NULL
                                                             AND (
                                                               tb1.NgayCapGCN > ?
                                                               OR (
                                                                 tb1.NgayCapGCN = ?
                                                                 AND tb1.SoGiay > ?
                                                               )
                                                             )
                
                                                           ORDER BY tb1.NgayCapGCN, tb1.SoGiay
                """.formatted(batchSize);

        List<Map<String, Object>> records = jdbcTemplate.queryForList(
                sql,
                lastNgayCap,
                lastNgayCap,
                lastSoGiay
        );

        attachThongSoVungHoatDong(records);
        return records;
    }

    private void attachThongSoVungHoatDong(List<Map<String, Object>> records) {
        List<String> soKiemSoats = records.stream()
                .map(record -> record.get("SoKiemSoat"))
                .filter(Objects::nonNull)
                .map(Object::toString)
                .distinct()
                .toList();

        if (soKiemSoats.isEmpty()) {
            return;
        }

        String placeholders = String.join(",", Collections.nCopies(soKiemSoats.size(), "?"));
        String sql = """
                SELECT
                  SoKiemSoat,
                  MaVungHD AS MaMuc,
                  ChieuChim,
                  F AS ManKho,
                  DW AS TrongTaiToanPhan,
                  LuongHang,
                  SoNguoiDuocCho
                FROM [dbo].[TS_VungHoatDong]
                WHERE SoKiemSoat IN (%s)
                """.formatted(placeholders);

        Map<String, List<Map<String, Object>>> thongSoBySoKiemSoat = jdbcTemplate
                .queryForList(sql, soKiemSoats.toArray())
                .stream()
                .collect(Collectors.groupingBy(
                        row -> Objects.toString(row.get("SoKiemSoat"), ""),
                        LinkedHashMap::new,
                        Collectors.toList()
                ));

        for (Map<String, Object> record : records) {
            String soKiemSoat = Objects.toString(record.get("SoKiemSoat"), "");
            record.put(
                    "__THONG_SO_VUNG_HOAT_DONG",
                    thongSoBySoKiemSoat.getOrDefault(soKiemSoat, List.of())
            );
        }
    }
}
