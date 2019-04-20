package br.ufsc.ppgcc.experion.extractor.input.engine.ecl;

import br.ufsc.ppgcc.experion.Experion;
import br.ufsc.ppgcc.experion.extractor.evidence.PhysicalEvidence;
import br.ufsc.ppgcc.experion.extractor.input.EvidenceSourceInput;
import br.ufsc.ppgcc.experion.extractor.input.engine.technique.ExtractionTechnique;
import br.ufsc.ppgcc.experion.extractor.input.BaseSourceInputEngine;
import br.ufsc.ppgcc.experion.model.expert.Expert;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

public class ECLEvidenceSourceInputEngine extends BaseSourceInputEngine implements Serializable {

    private transient Connection connection;

    public ECLEvidenceSourceInputEngine() throws SQLException, ClassNotFoundException {
        this(null, false);
    }

    public ECLEvidenceSourceInputEngine(ExtractionTechnique extractionTechnique, boolean connectToDatabase) {
        this.setExtractionTechnique(extractionTechnique);
        if (connectToDatabase) {
            try {
                this.connectToDatabase();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ECLEvidenceSourceInputEngine(ExtractionTechnique extractionTechnique) {
        this(extractionTechnique, false);
    }

    public void connectToDatabase() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            String url = "jdbc:postgresql://%s:%d/%s";
            Class.forName("org.postgresql.Driver");
            url = url.format(url, Experion.getInstance().getConfig().getString("ecl.db.host"), Experion.getInstance().getConfig().getInt("ecl.db.port"),
                    Experion.getInstance().getConfig().getString("ecl.db.database"));
            connection = DriverManager.getConnection(url, Experion.getInstance().getConfig().getString("ecl.db.user"), Experion.getInstance().getConfig().getString("ecl.db.password"));
        }
    }

    public void disconnectDatabase() throws SQLException {
        connection.close();
    }

    @Override
    public Set<Expert> getExpertEntities() {
        try {
            this.connectToDatabase();
            Set<Expert> entities = new HashSet<>();
            Statement st = null;
            st = connection.createStatement();
            ResultSet rs = st.executeQuery("select id,nome from professores order by id asc");
            while (rs.next()) {
                Expert expert = new Expert(rs.getString(1), rs.getString(2));
                expert.setIdentification(rs.getString(1));
                expert.setName(rs.getString(2));
                entities.add(expert);
            }
            rs.close();
            return entities;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<PhysicalEvidence> getNewEvidences(Expert expert, EvidenceSourceInput input) {

        String idInSource = expert.getIdentificationForSource(this.getEvidenceSource());

        try {
            this.connectToDatabase();
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }

        PreparedStatement st = null;
        Set<PhysicalEvidence> evidences = new HashSet<>();
        try {
            st = connection.prepareStatement("select _year, title, abstract from vw_document where id_prof = CAST(? AS INTEGER) order by _year");
            st.setString(1, idInSource);
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                PhysicalEvidence evidence = new PhysicalEvidence();
                evidence.setExpert(expert);
                Calendar cal = Calendar.getInstance();
                cal.set(Calendar.DAY_OF_MONTH, 1);
                cal.set(Calendar.MONTH, 0);
                cal.set(Calendar.YEAR, rs.getInt(1));
                evidence.setTimestamp(cal.getTime());
                evidence.setInput(input);
                String keywords = "";
                if (!StringUtils.isBlank(rs.getString(2))) {
                    keywords += rs.getString(2);
                }
                if (!StringUtils.isBlank(rs.getString(3))) {
                    keywords += " " + rs.getString(3);
                }
                keywords = keywords.trim();
                if (!StringUtils.isBlank(keywords)) {
                    evidence.addKeywords(keywords.split(" "));
                    evidences.add(evidence);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        evidences = customizeEvidences(expert, evidences);

        return evidences;
    }

    protected Set<PhysicalEvidence> customizeEvidences(Expert expert, Set<PhysicalEvidence> evidences) {
        return this.getExtractionTechnique().generateEvidences(expert, evidences, this.getLanguage());
    }

    @Override
    public Set<Expert> findExpertByName(String name) {
        try {
            this.connectToDatabase();
            Set<Expert> entities = new HashSet<>();
            PreparedStatement st = connection.prepareStatement("select id,nome from professores where lower(nome) like ? order by id asc");
            st.setString(1, "%" + name.toLowerCase() + "%");
            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                Expert expert = new Expert(rs.getString(1), rs.getString(2));
                expert.setIdentification(rs.getString(1));
                expert.setName(rs.getString(2));
                entities.add(expert);
            }
            rs.close();
            return entities;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
