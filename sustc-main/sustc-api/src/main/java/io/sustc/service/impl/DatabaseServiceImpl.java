package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12212255, 12210317);
    }


    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) throws SQLException {
        // TODO: implement your import logic
        try (Connection connection = dataSource.getConnection()) {
            for (DanmuRecord danmu : danmuRecords) {
                String sql = "INSERT INTO danmu (bv, content, time, post_time, post_mid, likeby_mid) VALUES (?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, danmu.getBv());
                    statement.setString(2, danmu.getContent());
                    statement.setString(3, String.valueOf(danmu.getTime()));
                    statement.setString(4, String.valueOf(danmu.getPostTime()));
                    statement.setString(5, String.valueOf(danmu.getMid()));
                    // Assuming likedBy is a String array, modify accordingly if it's a different type
                    statement.setString(6, Arrays.toString(danmu.getLikedBy()));
                    statement.execute();
                }
            }

            for (UserRecord user : userRecords) {
                String sql = "insert into users (mid, birthday, name, coin, sex, identity, level, qq, wechat, sign, password, following) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, String.valueOf(user.getMid()));
                    statement.setString(2, user.getBirthday());
                    statement.setString(3, user.getName());
                    statement.setInt(4, user.getCoin());
                    statement.setString(5, user.getSex());
                    // Assuming likedBy is a String array, modify accordingly if it's a different type
                    statement.setString(6, String.valueOf(user.getIdentity()));
                    statement.setInt(7, user.getLevel());
                    statement.setString(8, user.getQq());
                    statement.setString(9, user.getWechat());
                    statement.setString(10, user.getSign());
                    statement.setString(11, user.getPassword());
                    statement.setString(12, Arrays.toString(user.getFollowing()));
                    statement.execute();
                }
            }

            for (VideoRecord video : videoRecords) {
                String sql = " insert into videos (bv, title, duration, description, owner_mid, like_mid, coin_mid, favorite_mid, commit_time, reviewer_mid, review_time, public_time, watch_mid, watch_time, owner_name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, video.getBv());
                    statement.setString(2, video.getTitle());
                    statement.setFloat(3, video.getDuration());
                    statement.setString(4, video.getDescription());
                    statement.setString(5, String.valueOf(video.getOwnerMid()));
                    // Assuming likedBy is a String array, modify accordingly if it's a different type
                    statement.setString(6, Arrays.toString(video.getLike()));
                    statement.setString(7, Arrays.toString(video.getCoin()));
                    statement.setString(8, Arrays.toString(video.getFavorite()));
                    statement.setString(9, String.valueOf(video.getCommitTime()));
                    statement.setString(10, String.valueOf(video.getReviewer()));
                    statement.setString(11, String.valueOf(video.getReviewTime()));
                    statement.setString(12, String.valueOf(video.getPublicTime()));
                    statement.setString(13, Arrays.toString(video.getViewerMids()));
                    statement.setString(14, Arrays.toString(video.getViewTime()));
                    statement.setString(15, String.valueOf(video.getOwnerName()));
                    statement.execute();
                } catch (SQLException e) {
                    log.error("import failed", e);
                    e.printStackTrace();
                }

                System.out.println(danmuRecords.size());
                System.out.println(userRecords.size());
                System.out.println(videoRecords.size());
            }
        }
    }


    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
