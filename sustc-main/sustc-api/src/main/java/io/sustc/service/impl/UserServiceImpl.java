package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Random;

@Service
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    private static final String sql_user_register = """
            INSERT INTO users (mid, birthday, name, sex, identity, level, qq, wechat, sign, password)\s
            VALUES (?, ?, ?, ?, user, 0, ?, ?, ?, ?)""";

    @Override
    public long register(RegisterUserReq req) {
        try {
            Connection conn = dataSource.getConnection();
            PreparedStatement preparedStatement = null;
            preparedStatement = conn.prepareStatement(sql_user_register);
            preparedStatement.setLong(1, 123);
            preparedStatement.setString(2, req.getBirthday());
            preparedStatement.setString(3, req.getName());
            preparedStatement.setString(4, String.valueOf(req.getSex()));
            preparedStatement.setString(5, req.getQq());
            preparedStatement.setString(6, req.getWechat());
            preparedStatement.setString(7, req.getSign());
            preparedStatement.setString(8, req.getPassword());
            //判断密码等是否为空
            if (req.getPassword() == null || req.getPassword().isEmpty() ||
                    req.getName() == null || req.getName().isEmpty() ||
                    req.getSex() == null || req.getBirthday() == null || req.getBirthday().isEmpty()) {
                return -1;
            }

            //判断日期是否符合实际
            String birthdayString = req.getBirthday();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M月d日");

            try {
                // 解析字符串为 LocalDate
                LocalDate givenDate = LocalDate.parse(birthdayString, formatter);

                // 获取年月对象
                YearMonth yearMonth = YearMonth.of(givenDate.getYear(), givenDate.getMonth());

                // 获取该月的总天数
                int maxDaysInMonth = yearMonth.lengthOfMonth();

                // 判断给定日期是否在合法范围内
                if (givenDate.getDayOfMonth() <= maxDaysInMonth) {
                    System.out.println("日期合法，不超过当月天数");
                } else {
                    return -1;
                }
            } catch (Exception e) {
                System.out.println("日期格式解析错误：" + e.getMessage());
                return -1;
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return 123;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        return false;
    }

    @Override
    public boolean follow(AuthInfo auth, long followerMid) {
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        return null;
    }

}
