use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleResult {
    Pass,
    Fail,
    InfrastructureUnavailable,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairedSample {
    pub case_id: String,
    pub case_digest: String,
    pub seed: u64,
    pub grader_digest: String,
    pub runtime_digest: String,
    pub critical: bool,
    pub champion: SampleResult,
    pub challenger: SampleResult,
    pub champion_score_milli: i64,
    pub challenger_score_milli: i64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Better,
    Worse,
    Inconclusive,
    RefusedCriticalRegression,
    RefusedSmallSample,
    InvalidRewardIntegrity,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Statistics {
    pub decision: Decision,
    pub successful_pairs: u64,
    pub win_pairs: u64,
    pub loss_pairs: u64,
    pub delta_milli: i64,
    pub method: StatisticsMethod,
    pub input_digest: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatisticsMethod {
    PairedExactV1,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignalVector {
    pub required_pass: bool,
    pub negative_controls_pass: bool,
    pub reward_integrity_pass: bool,
}
pub fn reward_integrity(signals: &SignalVector) -> bool {
    signals.required_pass && signals.negative_controls_pass && signals.reward_integrity_pass
}
pub fn compare(samples: &[PairedSample], signals: &SignalVector, minimum_pairs: u64) -> Statistics {
    let mut ordered = samples.to_vec();
    ordered.sort_by(|a, b| {
        (
            a.case_id.as_str(),
            a.case_digest.as_str(),
            a.seed,
            a.runtime_digest.as_str(),
            a.grader_digest.as_str(),
        )
            .cmp(&(
                b.case_id.as_str(),
                b.case_digest.as_str(),
                b.seed,
                b.runtime_digest.as_str(),
                b.grader_digest.as_str(),
            ))
    });
    let mut out = Statistics {
        decision: Decision::Inconclusive,
        successful_pairs: 0,
        win_pairs: 0,
        loss_pairs: 0,
        delta_milli: 0,
        method: StatisticsMethod::PairedExactV1,
        input_digest: paired_digest(&ordered),
    };
    if !reward_integrity(signals) {
        out.decision = Decision::InvalidRewardIntegrity;
        return out;
    }
    let duplicate = ordered.windows(2).any(|x| {
        (
            x[0].case_id.as_str(),
            x[0].case_digest.as_str(),
            x[0].seed,
            x[0].runtime_digest.as_str(),
            x[0].grader_digest.as_str(),
        ) == (
            x[1].case_id.as_str(),
            x[1].case_digest.as_str(),
            x[1].seed,
            x[1].runtime_digest.as_str(),
            x[1].grader_digest.as_str(),
        )
    });
    if duplicate {
        out.decision = Decision::Inconclusive;
        return out;
    }
    for p in &ordered {
        if matches!(
            (p.champion, p.challenger),
            (SampleResult::InfrastructureUnavailable, _)
                | (_, SampleResult::InfrastructureUnavailable)
        ) {
            continue;
        };
        out.successful_pairs += 1;
        let d = p.challenger_score_milli - p.champion_score_milli;
        out.delta_milli += d;
        if p.champion == SampleResult::Pass && p.challenger == SampleResult::Fail {
            out.decision = if p.critical {
                Decision::RefusedCriticalRegression
            } else {
                Decision::Worse
            };
            return out;
        }
        if d > 0 {
            out.win_pairs += 1
        } else if d < 0 {
            out.loss_pairs += 1
        }
    }
    if out.successful_pairs < minimum_pairs {
        out.decision = Decision::RefusedSmallSample
    } else if out.win_pairs > out.loss_pairs && out.delta_milli > 0 {
        out.decision = Decision::Better
    } else if out.loss_pairs > out.win_pairs || out.delta_milli < 0 {
        out.decision = Decision::Worse
    };
    out
}
fn paired_digest(samples: &[PairedSample]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(
        serde_json::to_vec(samples).unwrap_or_default(),
    ))
}
