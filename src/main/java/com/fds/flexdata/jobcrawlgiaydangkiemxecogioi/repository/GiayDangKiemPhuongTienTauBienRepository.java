package com.fds.flexdata.jobcrawlgiaydangkiemxecogioi.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Repository
public class GiayDangKiemPhuongTienTauBienRepository {

    private final JdbcTemplate jdbcTemplate;

    public GiayDangKiemPhuongTienTauBienRepository(
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

                    TB_GiayChungNhan.SoGiayChungNhan
                        AS '__CHECKPOINT_SO_GIAY',

                    PARSENAME(
                        REPLACE(TB_Tau.SoDangKy, '-', '.'),
                        2
                    ) AS 'PhuongTien.MaDinhDanh',

                    TB_GiayChungNhan.SoGiayChungNhan
                        AS 'MaDinhDanh',

                    TB_GiayChungNhan.NgayCap
                        AS 'NgayCap',

                    TB_GiayChungNhan.NgayHetHan
                        AS 'NgayHetHan',

                    TB_GiayChungNhan.DonViCap
                        AS 'NoiCap.MaDinhDanh',

                    DMDonViDK_KhoiThuy.TenDonVi
                        AS 'NoiCap.TenToChuc',

                    TB_GiayChungNhan.NgayXacNhan
                        AS 'NgayKiemTraCuoiCung',

                    TB_GiayChungNhan.LanXacNhan
                        AS 'LanKiemTraCuoiCung',

                    TB_GiayChungNhan.LoaiGiayDangKiemTauBien
                        AS 'LoaiGiayDangKiemTauBien.MaMuc',

                    TB_GiayChungNhan.NoiKiemTra
                        AS 'NoiDangKiem',

                    TB_GiayChungNhan.TinhTrangHieuLucGiayTo
                        AS 'TinhTrangHieuLucGiayTo.MaMuc',

                    TB_Tau.SoPhanCap
                        AS 'PhuongTien.SoPhanCap',

                    TB_Tau.TenTau
                        AS 'PhuongTien.TenTau',

                    TB_Tau.SoIMO
                        AS 'PhuongTien.SoIMO',

                    TB_Tau.HoHieu
                        AS 'PhuongTien.HoHieu',

                    TB_Tau.MaNhomPhuongTienTauBien
                        AS 'PhuongTien.NhomPhuongTienTauBien.MaMuc',

                    TB_Tau.CongDungTauBien
                        AS 'PhuongTien.CongDungTauBien',

                    TB_Tau.TuyenKhaiThacTauBien
                        AS 'PhuongTien.TuyenKhaiThacTauBien.MaMuc',

                    TB_Tau.VungHoatDong
                        AS 'PhuongTien.VungHoatDong.MaMuc',

                    TB_Tau.KieuTau
                        AS 'PhuongTien.KieuTau',

                    TB_Tau.ChieuDaiLonNhat
                        AS 'PhuongTien.ChieuDaiLonNhat',

                    TB_Tau.ChieuRongLonNhat
                        AS 'PhuongTien.ChieuRongLonNhat',
                         
                    TB_Tau.HanCheVungHD
                        AS 'PhuongTien.HanCheVungHD',

                    TB_Tau.ChieuDai
                        AS 'PhuongTien.ChieuDaiThietKe',

                    TB_Tau.ChieuRong
                        AS 'PhuongTien.ChieuRongThietKe',

                    TB_Tau.ChieuCaoMan
                        AS 'PhuongTien.ChieuCaoMan',

                    TB_Tau.ChieuChim
                        AS 'PhuongTien.MonNuoc',

                    TB_Tau.TrongTaiToanPhan
                        AS 'PhuongTien.TrongTaiToanPhan',

                    TB_Tau.NamSanXuat
                        AS 'PhuongTien.NamSanXuat',

                    TB_Tau.NuocSanXuat
                        AS 'PhuongTien.NuocSanXuat.MaMuc',

                    TB_Tau.VatLieuVoTau
                        AS 'PhuongTien.VatLieuVoTau',

                    TB_Tau.TongDungTich
                        AS 'PhuongTien.TongDungTich',

                    TB_Tau.SucChoKhach
                        AS 'PhuongTien.SucChoKhach',

                    TB_Tau.DauHieuCapThanTau
                        AS 'PhuongTien.DauHieuCapThanTau',

                    TB_Tau.DauHieuCapMayTau
                        AS 'PhuongTien.DauHieuCapMayTau',

                    TB_Tau.SoLuongMayChinh
                        AS 'PhuongTien.SoLuongMayChinh',

                    TB_Tau.TongCongSuatMayChinh
                        AS 'PhuongTien.TongCongSuatMayChinh',

                    TB_Tau.CongSuatMayPhatDien
                        AS 'PhuongTien.CongSuatMayPhatDien',

                    TB_Tau.DungTichCoIch
                        AS 'PhuongTien.DungTichCoIch',

                    TB_Tau.NhaMayDongTau
                        AS 'PhuongTien.TenNhaMayDongTau',

                    TB_Tau.NamHoanCai
                        AS 'PhuongTien.NamHoanCai',

                    TB_Tau.NoiHoanCai
                        AS 'PhuongTien.NoiHoanCai'

                FROM dbo.TB_Tau

                INNER JOIN dbo.TB_GiayChungNhan
                    ON TB_Tau.SoPhanCap =
                       TB_GiayChungNhan.SoPhanCap

                LEFT JOIN dbo.DMDonViDK_KhoiThuy
                    ON TB_GiayChungNhan.DonViCap =
                       DMDonViDK_KhoiThuy.MaDonVi

                WHERE TB_Tau.SoDangKy LIKE '%%-%%-%%-%%'
                  AND TB_GiayChungNhan.NgayCap IS NOT NULL
                  AND (
                        TB_GiayChungNhan.NgayCap > ?
                        OR (
                            TB_GiayChungNhan.NgayCap = ?
                            AND TB_GiayChungNhan.SoGiayChungNhan > ?
                        )
                  )

                ORDER BY
                    TB_GiayChungNhan.NgayCap,
                    TB_GiayChungNhan.SoGiayChungNhan
                """.formatted(batchSize);

        return jdbcTemplate.queryForList(
                sql,
                lastNgayCap,
                lastNgayCap,
                lastSoGiay
        );
    }
}