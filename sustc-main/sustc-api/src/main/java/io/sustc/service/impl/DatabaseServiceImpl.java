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
        Connection connection = dataSource.getConnection();
        for (DanmuRecord danmu : danmuRecords) {
            StringBuilder sb = new StringBuilder("""
                    insert into danmu (bv, content, time, post_time, post_mid, likeby_mid) values
                    """);
            sb.append("('").append(danmu.getBv()).append("', '")
                    .append(danmu.getContent()).append("', $$")
                    .append(danmu.getTime()).append("$$, $$")
                    .append(danmu.getPostTime()).append("$$, $$")
                    .append(danmu.getMid()).append("$$, $$")
                    .append(Arrays.toString(danmu.getLikedBy())).append("$$), ");
            PreparedStatement statement = connection.prepareStatement(sb.substring(0, sb.length() - 1));
            statement.execute();
        }

        for (UserRecord user : userRecords) {
            StringBuilder sb = new StringBuilder("""
                    insert into users (mid, birthday, name, coin, sex, identity, level, qq, wechat, sign, password, following) values
                    """);
            sb.append(user.getMid()).append("', '")
                    .append(user.getBirthday()).append("', $$")
                    .append(user.getName()).append("$$, $$")
                    .append(user.getCoin()).append("$$, $$")
                    .append(user.getSex()).append("$$, $$")
                    .append(user.getIdentity()).append("$$, $$")
                    .append(user.getLevel()).append("$$, $$")
                    .append(user.getQq()).append("$$, $$")
                    .append(user.getWechat()).append("$$, $$")
                    .append(user.getSign()).append("$$, $$")
                    .append(user.getPassword()).append("$$, $$")
                    .append(Arrays.toString(user.getFollowing())).append("$$),");
            PreparedStatement statement = connection.prepareStatement(sb.substring(0, sb.length() - 1));
            statement.execute();
        }

        for (VideoRecord video : videoRecords) {
            StringBuilder sb = new StringBuilder("""
                    insert into videos (bv, title, duration, description, owner_mid, like_mid, coin_mid, favorite_mid, commit_mid, reviewer_mid, review_time, public_time, watch_mid, watch_time) values\s
                    """);
            sb.append(video.getBv()).append("', '")
                    .append(video.getTitle()).append("$$, $$")
                    .append(video.getDuration()).append("$$, $$")
                    .append(video.getDescription()).append("$$, $$")
                    .append(video.getOwnerMid()).append("$$, $$")
                    .append(Arrays.toString(video.getLike())).append("$$, $$")
                    .append(Arrays.toString(video.getCoin())).append("$$, $$")
                    .append(Arrays.toString(video.getFavorite())).append("$$, $$")
                    .append(video.getOwnerMid()).append("$$, $$")
                    .append(video.getReviewer()).append("$$, $$")
                    .append(video.getReviewTime()).append("$$, $$")
                    .append(video.getPublicTime()).append("$$, $$")
                    .append(Arrays.toString(video.getViewerMids())).append("$$, $$")
                    .append(Arrays.toString(video.getViewTime())).append("$$),");
            PreparedStatement statement = connection.prepareStatement(sb.substring(0, sb.length() - 1));
            statement.execute();
        }


        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
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
