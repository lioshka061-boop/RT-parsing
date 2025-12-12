use crate::external_import::{Item, Offer, Vendored};
use crate::SELF_ADDR;
use crate::{dt, tt};
use crate::{parse_vendor_from_link, uploader};
use actix::prelude::*;
use actix_broker::BrokerSubscribe;
use anyhow::anyhow;
use anyhow::Context as AnyhowContext;
use currency_service::{CurrencyService, ListRates};
use derive_more::Display;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use log_error::LogError;
use once_cell::sync::Lazy;
use reqwest::Client;
use rt_types::access::UserCredentials;
use rt_types::category::{self, By};
use rt_types::product::{Product, UaTranslation};
use rt_types::shop::service::ShopService;
use rt_types::shop::ConfigurationChanged;
use rt_types::shop::{
    self, ExportEntry, ExportEntryLink, ExportOptions, FileFormat, ParsingCategoriesAction, Shop,
};
use rt_types::subscription::service::UserSubscription;
use rt_types::watermark::service::WatermarkUpdated;
use rt_types::Availability;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::{broadcast, Notify, RwLock};
use typesafe_repository::IdentityOf;
use xxhash_rust::xxh64::xxh64;

pub const MAX_RETRY_COUNT: usize = 30;

static PL_ARTICLES: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    let raw = r#"
33021
DT00336
DT04013
DT03817
DT04014
11111
65238
DT02359
DT00409
DT00401
DT02359
DT00409
22117
48256
20383
DT00801-2
DT00699
20300
24585
32055
13004
13005
DT00573
13006
13055
13008
DT00700
DT03754
32568
39423
39422
11162
12010
24588
32055
13004
31104
13005
DT00573
11162
25492
25491
12010
24588
22121
22120
32126
25488
10351
10352
25490
20355
20355
20367
20367
20382
20379
20353
20371
10343
20353
20371
22121
22120
32126
25488
20371
24573
DT00801-2
DT00448
DT03811
23548
42900
42788
42905
13009
13010
13011
DT00735
DT00024
68011
20372
13013
68009
13014
13015
13010
13011
DT05635
DT00737
68011
20372
42900
42788
42905
DT00735
13016
20372
13013
68009
13014
DT05635
13016
20372
27898
20854
27898
20957
27873
24572
21457
27897
27874
DT00259
DT00532
DT00464
DT00261
DT00485
DT00321
DT00469
DT00494
13018
DT00465
13021
DT00485
DT00321
13020
13021
DT00734
22123
22122
48005
25487
48003
48004
DT01216
DT01217
48002
48006
23145
23146
DT00899
48001
44128
44122
23674
48005
25487
23149
48011
23147
44128
48002
DT00899
23150
23156
23155
23154
23152
26584
23157
23158
23159
23161
DT01219
13147
DT00760
456987
DT00474
25143
25144
23563
DT03665
39412
23674
25153
25152
25156
25154
DT01219
13147
DT00760
456987
21462
21461
25158
DT01220
DT00900
10900
39412
DT03665
23674
39417
21463
25157
DT01220
DT00900
10900
25585
25158
25507
51878
51474
27862
27865
27864
27863
27860
27866
27864
27863
27864
27863
13019
76583
12019
DT00763
24025
24028
22146
22148
22152
22153
22158
12541
12540
23162
12015
DT00897
DT04015
DT00776
DT00801
DT00808
DT01192
DT00798
DT00872
12205
36025
36024
24582
24584
DT36026
DT36027
DT02422
DT00855
DT00325
DT00914
DT00869
DT00862
DT00913
DT00873
27883
12021
DT01205
DT00762
DT00902
13026
DT00766
13027
DT01204
23488
23487
27884
12022
13029
12035
DT00905
13030
25145
22105
22104
22116
DT22173
DT22174
DT00600
39434
20378
16022
16023
16025
39435
39414
39436
39425
27876
23163
10354
25455
39437
25456
25457
25455
25456
25457
27868
25455
25456
25457
12017
DT01184
DT00780
22119
39438
DT00844
DT01119
DT01871
DT01200
DT00841
DT00836
DT00835
DT00834
24589
20358
70008
70007
DT00896
23855
23856
28258
28257
25160
DT00333
DT00334
DT00335
22098
39418
39419
39420
27879
24577
16027
16026
27852
27853
27855
27856
24574
22012
DT00770
20365
21459
70004
70003
DT00767
71001
39424
22115
20191
20585
39424
DT00767
71001
12170
21087
45874
21064
27877
27857
27858
70006
70011
12459
70013
22061
39427
39442
47777
22088
22089
DT02640
12036
DT00988
DT01202
DT00978
DT00964
DT00989
DT00990
DT01849
DT01853
12589
DT01859
DT01855
DT02158
DT01070
DT02193
DT02192
DT02193
DT01256
DT01257
DT01254
DT01255
DT00525
DT01254
23486
DT04789
DT01260
DT01258
DT01259
DT03254
DT01274
DT01269
DT00203
DT01275
DT01277
DT01258
DT01259
DT00201
DT03254
DT01274
DT01269
DT00203
14256
14569
25698
DT01288
DT01202
DT00978
DT00964
DT01303
DT01294
DT01307
DT01269
DT00203
13254
DT02204
DT01312
DT02306
DT01327
DT02295
DT02300
11015
DT01317
DT02401
DT01337
DT01338
DT01336
DT01339
DT02327
DT01335
DT02436
42910
DT01535
DT01530
DT01532
DT00237
DT01553
DT01553
DT01565
DT00215
DT01432
DT01431
DT01565
DT01442
DT01432
DT01431
12037
DT03787
DT09064
DT09063
24578
DT02347
DT01576
DT02139
DT01574
DT02347
DT01576
DT02139
DT01584
DT01588
DT01596
DT02145
DT01596
DT01573
DT01586
DT01587
DT01585
DT01588
DT01596
DT02145
DT01596
DT01573
DT01590
DT01587
DT01585
DT03270
DT03278
DT01580
DT00966
DT03487
DT09875
DT00970
DT00967
DT00971
DT00980
DT00983
DT01617
20176
DT22172
24579
DT01770
DT01768
DT01771
DT01767
DT01766
10609
27881
DT02387
DT01008
DT01010
DT01009
DT01009
DT01004
DT01013
DT01019
DT01009
DT01009
DT01796
DT01796
DT03777
DT01780
DT03777
DT01780
DT01808
DT01940
DT01946
10986
DT00984
DT00985
12061
12062
DT01936
DT00987
DT01934
DT01932
DT00673
DT01926
DT01925
DT03474
12061
12062
DT01936
DT00987
DT02348
DT01957
DT02028
22026
12088
12201
12202
12203
22167
24571
DT02032
23045
DT02067
10357
22112
DT02034
20186
39439
39409
39441
39439
10353
10371
20567
DT00982
DT00609
10610
DT01046
11046
24576
13050
23514
21200
21202
21302
21303
25301
26607
DT01048
DT01049
DT01050
DT01051
23101
DT00999
221004
22113
22203
DT02430
DT02061
24575
12067
DT02064
DT00598
63801
103689
44701
22164
DT22171
DT00598
103689
235148
25020
12023
DT02125
DT02128
DT02148
DT02131
22118
DT02190
DT02228
DT05001
DT05003
13056
DT05040
DT05033
DT05008
DT05009
DT05211
DT05155
DT05327
DT05183
DT05096
13062
DT05201
DT05219
DT05198
DT05228
13063
DT05322
13058
13059
13060
DT05316
15157
DT05394
13064
13065
DT05389
12104
DT01045
DT05209
11149
DT05383
DT05389
12104
DT01045
DT05208
11149
13066
DT01052
11149
DT05408
DT01052
11149
10362
25146
27859
DT05442
DT05607
DT05660
DT05661
13070
DT00821
27882
DT01053
25864
25865
DT05864
DT05338
DT05851
DT05871
DT05869
DT05888
DT05866
DT05876
DT05864
DT05338
DT05874
DT05869
DT06277
DT06347
DT06278
DT06356
DT06346
DT06277
DT06357
DT06278
27978
25866
DT05982
25868
DT05964
DT06099
DT06057
DT06123
DT06161
DT06178
DT06163
DT06213
DT06227
DT06168
DT06175
DT06225
DT06270
DT06283
DT06347
DT06278
DT06356
DT06346
13075
13073
DT06270
DT06283
DT06357
DT06278
13075
13073
27978
DT06368
DT06373
13076
DT06378
13078
13077
13079
DT06378
13085
39410
DT06450
DT06430
DT06574
DT06490
DT06487
DT06470
25873
DT05306
DT01054
DT06631
DT06629
13081
13080
22114
DT06715
DT06763
DT01202
DT00978
DT00964
DT05489
DT05479
DT05494
25877
25875
25878
25876
25875
DT01055
DT06790
DT06876
DT06560
DT06902
DT06959
DT06958
DT07000
DT07001
DT07011
DT06971
DT06970
DT06977
DT06980
DT06982
DT07121
DT07119
DT07089
DT07039
DT07043
DT07119
DT07089
22150
28722
DT07143
DT06959
DT06958
DT07156
16468
DT01059
DT07168
DT07177
DT01060
DT01061
24587
12027
34698
24587
12027
22165
DT07176
DT07170
DT07174
DT07182
DT05698
DT07170
DT07174
DT09064
DT09065
DT09063
DT09064
DT07392
DT07395
DT07331
DT07237
DT07331
DT07238
DT07236
DT07277
DT07270
DT07331
DT07238
DT07236
DT07277
13086
DT07270
25512
25514
DT07471
DT07474
DT07479
DT07476
DT07396
12028
DT07392
DT07395
DT01062
25869
25515
25871
12033
24591
DT07482
DT07481
DT07471
DT07474
DT07479
DT07476
DT07392
DT07395
DT07492
13178
DT22177
39426
27872
27869
DT22179
DT22181
27872
13179
13182
13184
DT22179
DT07506
27880
27875
DT07520
DT07519
DT07520
12368
DT01064
DT01063
25506
13168
26784
25147
39432
13166
13167
24594
24593
22157
25511
DT01065
10358
10360
10361
10153
13164
25505
27867
13162
DT07541
DT03472
DT03821
DT01044
DT07562
DT03820
DT01072
DT01073
DT07675
DT01074
DT07634
DT00779
DT01075
DT01076
DT01077
24581
24592
DT01473
DT07957
DT07955
13097
DT01078
DT01079
DT01080
12169
10818
44011
DT07964
DT01082
DT01081
DT07981
DT01084
DT07922
DT07981
DT08006
DT08031
26852
DT08049
DT08283
DT08189
DT08181
DT08288
DT08184
DT08291
36102
DT02567
DT08356
13103
DT08500
DT08490
13104
DT08392
13105
DT08504
13106
DT08525
DT08487
DT08498
DT08485
DT08510
13136
DT08349
DT08589
23044
DT08598
DT08600
DT08587
13148
13149
11161
13155
22100
13150
13150
13155
39433
22111
13108
DT08598
DT01086
DT01087
DT01085
13150
25450
12204
20959
25498
25504
20960
25496
36104
36106
36109
22160
11161
22151
DT08684
DT08685
DT08682
DT08683
DT08682
DT08683
DT08793
DT08820
13116
DT08783
DT08794
13151
13115
DT08784
DT08867
DT08793
DT08820
13116
DT08856
13114
DT02196
13151
13115
24521
DT08887
DT08867
10815
13119
13120
44001
44000
DT01857
37000
13123
13124
13125
DT01083
21564
44003
44004
22107
22106
23043
DT01345
12169
10818
44011
DT09058
DT08967
DT09040
DT09058
DT09035
DT08919
39413
24583
DT09064
DT09065
DT09063
DT09064
DT09116
DT09112
DT09080
DT09094
DT09101
DT09093
DT09115
DT09102
DT09149
13126
DT01347
DT01349
DT09148
13127
DT00701
DT01351
13128
13130
13129
DT09147
DT02345
13131
DT01353
22109
12099
DT09149
13126
DT01347
DT01349
13128
13130
13129
22108
22109
25500
13132
13131
13133
13134
13126
DT01347
13158
13144
13156
13143
13157
13159
13160
12084
13161
DT01357
DT01355
DT01355
60000
20000
DT04563
DT04564
DT04564
20100
12012
36113
36114
10561
22159
DT01361
DT01079
DT01080
DT01365
DT09186
DT08288
DT09167
39415
39416
DT08291
36102
DT02569
DT07740
DT07742
DT02389
DT07766
DT07861
DT07791
DT07780
DT07777
DT07779
DT07794
DT07773
DT07791
DT07779
DT07794
13135
13139
DT07785
DT07781
13140
13141
DT07837
DT07842
DT07834
DT07836
DT07850
DT07848
DT07823
DT07837
DT07842
DT07835
DT07846
DT07836
DT07850
DT07848
DT07823
74128
54784
22103
22102
22101
22110
34970
39411
DT07859
DT07766
DT07861
DT22176
DT07880
DT07780
DT07777
DT07779
DT07794
DT07773
DT07880
DT07779
DT07794
13135
65321
DT07905
DT07781
13140
13141
22103
22102
22101
22110
34970
39411
24569
DT07766
DT07861
DT07906
DT07850
DT07848
DT07823
DT07906
DT01247
DT07850
DT07848
DT07823
13569
13568
DT04569
DT04568
DT04567
DT07906
DT07850
DT07848
13569
12458
DT07877
    "#;
    raw.split_whitespace().filter(|c| *c != "нема").collect()
});

pub struct ExportService {
    client: Client,
    entries: Vec<(IdentityOf<Shop>, ExportEntry)>,
    tt_repo: Arc<dyn tt::product::ProductRepository>,
    trans_repo: Arc<dyn tt::product::TranslationRepository>,
    dt_repo: Arc<dyn dt::product::ProductRepository>,
    davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
    category_repo: Arc<dyn category::CategoryRepository>,
    shop_service: Addr<ShopService>,
    currency_service: Addr<CurrencyService>,
    export: HashMap<String, Arc<RwLock<Export>>>,
}

impl ExportService {
    pub fn new(
        client: Client,
        entries: Vec<(IdentityOf<Shop>, ExportEntry)>,
        tt_repo: Arc<dyn tt::product::ProductRepository>,
        trans_repo: Arc<dyn tt::product::TranslationRepository>,
        dt_repo: Arc<dyn dt::product::ProductRepository>,
        davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
        category_repo: Arc<dyn category::CategoryRepository>,
        shop_service: Addr<ShopService>,
        currency_service: Addr<CurrencyService>,
    ) -> Self {
        Self {
            client,
            entries,
            tt_repo,
            trans_repo,
            dt_repo,
            davi_repo,
            category_repo,
            shop_service,
            currency_service,
            export: HashMap::new(),
        }
    }
    pub async fn start_export_cycle(
        client: Client,
        export: Arc<RwLock<Export>>,
        dt_repo: Arc<dyn dt::product::ProductRepository>,
        tt_repo: Arc<dyn tt::product::ProductRepository>,
        davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
        category_repo: Arc<dyn category::CategoryRepository>,
        trans_repo: Arc<dyn tt::product::TranslationRepository>,
        currency_service: Addr<CurrencyService>,
    ) {
        let (mut entry, start_notify, stop_notify, mut shop, mut rx) = {
            let e = export.read().await;
            (
                e.entry.clone(),
                e.start.clone(),
                e.stop.clone(),
                e.shop.clone(),
                e.suspend_tx.subscribe(),
            )
        };
        let mut file_name = entry.file_name(FileFormat::Csv);
        let mut retry_count = 0;
        match tokio::fs::metadata(format!("./export/{shop}/{file_name}"))
            .await
            .map(|m| m.modified())
        {
            Ok(Ok(m)) => match std::time::SystemTime::now().duration_since(m) {
                Ok(d) if d < entry.update_rate => {
                    {
                        let mut export = export.write().await;
                        export.status = ExportStatus::Success;
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(entry.update_rate - d) => (),
                        _ = start_notify.notified() => (),
                        _ = stop_notify.notified() => return,
                    }
                }
                Ok(_) => (),
                Err(err) => {
                    log::error!("Unable to calculate duration since last modified: {err}");
                }
            },
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
            Ok(Err(err)) | Err(err) => {
                log::error!("Unable to read export file metadata: {err}");
            }
        }
        loop {
            {
                let mut export = export.write().await;
                export.status = ExportStatus::Enqueued;
                if entry != export.entry {
                    entry = export.entry.clone();
                    file_name = entry.file_name(None);
                }
                shop = export.shop;
                if let Some(true) = rx.try_recv().log_error("Unable to read suspend rx") {
                    export.status = ExportStatus::Suspended;
                    drop(export);
                    loop {
                        match rx.recv().await.log_error("Unable to read suspend rx") {
                            Some(false) => break,
                            _ => continue,
                        }
                    }
                    continue;
                }
            }
            let permit = match SEMAPHORE.acquire().await {
                Ok(p) => Some(p),
                Err(err) => {
                    log::warn!("Unable to acquire semaphore permit: {err}");
                    None
                }
            };
            log::info!("Generating {file_name}");
            let shop_id = shop.to_string();
            let (res, _) = tokio::join!(
                do_export(
                    &entry,
                    shop,
                    client.clone(),
                    &shop_id,
                    dt_repo.clone(),
                    tt_repo.clone(),
                    davi_repo.clone(),
                    category_repo.clone(),
                    trans_repo.clone(),
                    currency_service.clone(),
                ),
                async {
                    let mut export = export.write().await;
                    export.status = ExportStatus::InProgress;
                }
            );
            drop(permit);
            let status = match res {
                Ok(_) => {
                    log::info!("{file_name} has been generated");
                    retry_count = 0;
                    ExportStatus::Success
                }
                Err(ExportError::Download(_, uploader::DownloadFromLinkError::Other(err))) => {
                    log::error!("Unable to generate {file_name}: {err}");
                    if retry_count < MAX_RETRY_COUNT {
                        retry_count += 1;
                        continue;
                    } else {
                        retry_count = 0;
                        ExportStatus::Failure(err.to_string())
                    }
                }
                Err(ExportError::Download(
                    l,
                    uploader::DownloadFromLinkError::UnableToParse { err, content },
                )) => {
                    let hash = &xxh64(l.as_bytes(), l.len() as u64);
                    if let Err(err) = std::fs::write(format!("{hash:x}.xml.tmp"), content) {
                        log::error!("Unable to write unparsed content of link {}: {err}", l);
                    } else {
                        log::error!("Unable to parse link: {err}\nUnparsed content of link {} written as {hash:x}", l);
                    }
                    ExportStatus::Failure(err.to_string())
                }
                Err(err) => {
                    log::error!("Unable to generate {file_name}: {err}");
                    ExportStatus::Failure(err.to_string())
                }
            };
            {
                let mut export = export.write().await;
                export.status = status;
            }
            tokio::select! {
                _ = tokio::time::sleep(entry.update_rate) => (),
                _ = start_notify.notified() => (),
                _ = stop_notify.notified() => return,
            }
        }
    }
}

pub struct AddExportPermission(IdentityOf<Shop>);

impl AddExportPermission {
    pub fn acquire(
        user: &UserCredentials,
        shop: &Shop,
        subscription: &Option<UserSubscription>,
    ) -> Option<Self> {
        if shop.owner != user.login {
            return None;
        }
        if subscription.as_ref().is_some_and(|sub| {
            shop.export_entries.len() > sub.inner().limits.maximum_exports as usize
        }) {
            return None;
        }
        Some(Self(shop.id))
    }
    pub fn shop_id(&self) -> &IdentityOf<Shop> {
        &self.0
    }
}

pub struct UpdateExportEntryPermission(ExportEntry, String);

impl UpdateExportEntryPermission {
    pub fn acquire(
        export: ExportEntry,
        hash: String,
        subscription: &Option<UserSubscription>,
    ) -> Option<Self> {
        if export
            .links
            .as_ref()
            .zip(subscription.as_ref())
            .is_some_and(|(l, s)| l.len() >= s.inner().limits.links_per_export as usize)
        {
            return None;
        }
        Some(Self(export, hash))
    }
    pub fn into_inner(self) -> (ExportEntry, String) {
        (self.0, self.1)
    }
}

#[derive(Message)]
#[rtype(result = "Option<Export>")]
pub struct GetStatus(pub String);

#[derive(Message)]
#[rtype(result = "HashMap<String, Export>")]
pub struct GetAllStatus(pub IdentityOf<Shop>);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Add(pub AddExportPermission, pub ExportEntry);

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct Remove(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Start(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct StartAll;

#[derive(Message)]
#[rtype(result = "Result<String, anyhow::Error>")]
pub struct Update(pub IdentityOf<Shop>, pub UpdateExportEntryPermission);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Cleanup;

#[derive(Message)]
#[rtype(result = "Result<(), anyhow::Error>")]
pub struct SuspendByShop(pub IdentityOf<Shop>, pub bool);

#[derive(Debug, Clone)]
pub struct Export {
    pub shop: IdentityOf<Shop>,
    pub status: ExportStatus,
    pub entry: ExportEntry,
    start: Arc<Notify>,
    suspend_tx: broadcast::Sender<bool>,
    stop: Arc<Notify>,
}

impl Export {
    pub fn status(&self) -> &ExportStatus {
        &self.status
    }
    pub fn entry(&self) -> &ExportEntry {
        &self.entry
    }
}

#[derive(Clone, Debug, Display, Serialize)]
pub enum ExportStatus {
    #[display("В очереди")]
    Enqueued,
    #[display("В процессе")]
    InProgress,
    #[display("Экспорт успешно завершен")]
    Success,
    #[display("Экспорт приостановлен")]
    Suspended,
    #[display("Экспорт завершен с ошибкой: {:?}", _0)]
    Failure(String),
}

impl Actor for ExportService {
    type Context = Context<Self>;

    fn start(mut self) -> Addr<Self>
    where
        Self: Actor<Context = Context<Self>>,
    {
        for (shop, entry) in &self.entries {
            let (suspend_tx, _) = broadcast::channel(20);
            self.export.insert(
                entry.generate_hash().to_string(),
                Arc::new(RwLock::new(Export {
                    shop: *shop,
                    entry: entry.clone(),
                    start: Arc::new(Notify::new()),
                    stop: Arc::new(Notify::new()),
                    suspend_tx,
                    status: ExportStatus::InProgress,
                })),
            );
        }
        for e in self.export.values() {
            tokio::task::spawn_local(Self::start_export_cycle(
                self.client.clone(),
                e.clone(),
                self.dt_repo.clone(),
                self.tt_repo.clone(),
                self.davi_repo.clone(),
                self.category_repo.clone(),
                self.trans_repo.clone(),
                self.currency_service.clone(),
            ));
        }
        Context::new().run(self)
    }

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.subscribe_system_async::<ConfigurationChanged>(ctx);
        self.subscribe_system_async::<WatermarkUpdated>(ctx);
        ctx.address().do_send(Cleanup);
    }
}

impl Handler<ConfigurationChanged> for ExportService {
    type Result = ();

    fn handle(&mut self, _msg: ConfigurationChanged, _ctx: &mut Self::Context) -> Self::Result {
        // self.entries = msg.0.export_entries;
    }
}

impl Handler<WatermarkUpdated> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(
        &mut self,
        WatermarkUpdated { shop_id, from, to }: WatermarkUpdated,
        _: &mut Self::Context,
    ) -> Self::Result {
        let entries = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            for e in entries {
                let mut e = e.write().await;
                if e.shop != shop_id {
                    continue;
                }
                let entry = &mut e.entry;
                entry
                    .links
                    .iter_mut()
                    .flatten()
                    .filter_map(|l| l.options.as_mut())
                    .chain(entry.tt_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.dt_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.jgd_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.pl_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.skm_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.dt_tt_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.maxton_parsing.iter_mut().map(|o| &mut o.options))
                    .chain(entry.davi_parsing.iter_mut())
                    .filter_map(|o| o.watermarks.as_mut())
                    .filter(|(n, _)| *n == from)
                    .for_each(|(n, _)| *n = to.clone());
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GetStatus> for ExportService {
    type Result = ResponseActFuture<Self, Option<Export>>;

    fn handle(&mut self, GetStatus(hash): GetStatus, _ctx: &mut Self::Context) -> Self::Result {
        let export = self.export.get(&hash).cloned();
        let fut = async move {
            if let Some(export) = export {
                Some(export.read().await.clone())
            } else {
                None
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<GetAllStatus> for ExportService {
    type Result = ResponseActFuture<Self, HashMap<String, Export>>;

    fn handle(
        &mut self,
        GetAllStatus(shop): GetAllStatus,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let export = self.export.clone();
        let fut = async move {
            let mut res = HashMap::new();
            for (h, e) in export.iter() {
                let e = e.read().await;
                if e.shop == shop {
                    res.insert(h.clone(), e.clone());
                }
            }
            res
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Start> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, Start(hash): Start, _ctx: &mut Self::Context) -> Self::Result {
        let export = self.export.get(&hash).cloned();
        let fut = async move {
            if let Some(n) = export {
                n.read().await.start.notify_waiters()
            } else {
                log::warn!("Export entry {hash} not found");
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<StartAll> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: StartAll, _ctx: &mut Self::Context) -> Self::Result {
        let export = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            for e in export {
                e.read().await.start.notify_waiters();
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Update> for ExportService {
    type Result = ResponseActFuture<Self, Result<String, anyhow::Error>>;

    fn handle(
        &mut self,
        Update(shop, permission): Update,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let (entry, hash) = permission.into_inner();
        let self_addr = ctx.address().clone();
        let h = hash.clone();
        let addr = self.shop_service.clone();
        let ent = entry.clone();
        let new_hash = ent.generate_hash();
        let nh = new_hash.clone();

        let ex = self.export.get(&hash).cloned();
        let fut = async move {
            let mut shop = addr
                .send(shop::service::Get(shop))
                .await?
                .context("Unable to read shop")?
                .ok_or(anyhow::anyhow!("Shop not found"))?;
            let e = shop
                .export_entries
                .iter_mut()
                .find(|e| e.generate_hash().to_string() == h);
            if let Some(e) = e {
                *e = ent.clone();
            } else {
                log::warn!("Entry not found in config");
                shop.export_entries.push(ent.clone())
            }
            addr.send(shop::service::Update(shop))
                .await?
                .context("Unable to update shop")?;
            if let Some(export) = ex {
                {
                    let mut ex = export.write().await;
                    ex.entry = ent.clone();
                }
            } else {
                log::warn!("Entry not found in export");
            }
            self_addr.do_send(Cleanup);
            Ok(nh.to_string())
        };
        Box::pin(fut.into_actor(self).map(move |res, act, _ctx| {
            if res.is_err() {
                return res;
            }
            let export = act.export.remove(&hash);
            if let Some(export) = export {
                act.export.insert(new_hash.to_string(), export);
            }
            res
        }))
    }
}

impl Handler<Add> for ExportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Add(shop, entry): Add, _: &mut Context<Self>) -> Self::Result {
        let addr = self.shop_service.clone();
        let shop = *shop.shop_id();
        let (suspend_tx, _) = broadcast::channel(20);
        let e = Arc::new(RwLock::new(Export {
            shop,
            entry: entry.clone(),
            start: Arc::new(Notify::new()),
            stop: Arc::new(Notify::new()),
            suspend_tx,
            status: ExportStatus::InProgress,
        }));
        let client = self.client.clone();
        let dt_repo = self.dt_repo.clone();
        let tt_repo = self.tt_repo.clone();
        let davi_repo = self.davi_repo.clone();
        let trans_repo = self.trans_repo.clone();
        let category_repo = self.category_repo.clone();
        let currency_service = self.currency_service.clone();
        let new_entry = entry.clone();
        let fut = async move {
            let mut shop = addr
                .send(shop::service::Get(shop))
                .await?
                .context("Unable to read shop")?
                .ok_or(anyhow::anyhow!("Shop not found"))?;
            shop.export_entries.push(new_entry);
            addr.send(shop::service::Update(shop)).await??;
            Ok(())
        };
        Box::pin(fut.into_actor(self).map(move |res, act, _| {
            act.export
                .insert(entry.generate_hash().to_string(), e.clone());
            tokio::task::spawn_local(Self::start_export_cycle(
                client,
                e,
                dt_repo,
                tt_repo,
                davi_repo,
                category_repo,
                trans_repo,
                currency_service,
            ));
            res
        }))
    }
}

impl Handler<Remove> for ExportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(&mut self, Remove(hash): Remove, ctx: &mut Context<Self>) -> Self::Result {
        let addr = self.shop_service.clone();
        let self_addr = ctx.address().clone();
        let e = self.export.remove(&hash);
        let fut = async move {
            let shop;
            if let Some(e) = e {
                let e = e.read().await;
                e.stop.notify_waiters();
                shop = e.shop;
            } else {
                return Err(anyhow::anyhow!("Export entry worker not found"));
            }
            let mut shop = addr
                .send(shop::service::Get(shop))
                .await??
                .ok_or(anyhow::anyhow!("Shop not found"))?;
            let e = shop
                .export_entries
                .iter_mut()
                .enumerate()
                .find(|(_, e)| e.generate_hash().to_string() == hash);
            if let Some((i, _)) = e {
                shop.export_entries.remove(i);
            }
            addr.send(shop::service::Update(shop))
                .await?
                .context("Unable to update shop")?;
            self_addr.do_send(Cleanup);
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<Cleanup> for ExportService {
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, _: Cleanup, _: &mut Context<Self>) -> Self::Result {
        let entries = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            let entries = stream::iter(entries)
                .map(|e| async move {
                    let e = e.read().await;
                    (e.shop, e.entry.clone())
                })
                .buffered(10)
                .collect::<Vec<_>>()
                .await;
            if let Err(err) = cleanup(entries) {
                log::error!("Unable to perform cleanup: {err:?}");
            }
        };
        Box::pin(fut.into_actor(self))
    }
}

impl Handler<SuspendByShop> for ExportService {
    type Result = ResponseActFuture<Self, Result<(), anyhow::Error>>;

    fn handle(
        &mut self,
        SuspendByShop(shop, val): SuspendByShop,
        _: &mut Self::Context,
    ) -> Self::Result {
        let export = self.export.values().cloned().collect::<Vec<_>>();
        let fut = async move {
            for e in export {
                let e = e.read().await;
                if e.shop == shop {
                    println!("suspend: {val}");
                    e.suspend_tx.send(val)?;
                }
            }
            Ok(())
        };
        Box::pin(fut.into_actor(self))
    }
}

pub fn cleanup<E: IntoIterator<Item = (IdentityOf<Shop>, ExportEntry)>>(
    entries: E,
) -> Result<(), anyhow::Error> {
    let map = entries
        .into_iter()
        .map(|(s, e)| (s, e.file_name(None)))
        .into_group_map();
    for (shop_id, file_names) in map {
        for d in std::fs::read_dir(format!("export/{shop_id}"))? {
            let d = d?;
            let file_name = d
                .file_name()
                .to_str()
                .map(ToString::to_string)
                .ok_or_else(|| anyhow!("Unable to convert file name"))?;
            if !d.metadata()?.is_dir()
                && !file_names
                    .iter()
                    .any(|f| f.contains(&file_name) || file_name.contains(f))
            {
                std::fs::remove_file(d.path())?;
            }
        }
    }
    Ok(())
}

pub async fn parse_from_links(
    l: &[ExportEntryLink],
    client: reqwest::Client,
) -> Result<
    (
        HashMap<ExportEntryLink, Vec<Offer>>,
        HashMap<ExportEntryLink, Vec<Item>>,
    ),
    ExportError,
> {
    let ext = futures::stream::iter(l.iter())
        .map(|l| {
            let client = client.clone();
            async move {
                match uploader::download_from_link(&l.link, client).await {
                    Ok(items) => Ok((l, items)),
                    Err(err) => Err(ExportError::Download(l.link.clone(), err)),
                }
            }
        })
        .buffer_unordered(1024)
        .try_collect::<Vec<_>>()
        .await?;
    let (off, ext): (Vec<_>, Vec<_>) = ext.into_iter().partition_map(|(v, p)| match p {
        uploader::DownloadResult::Offers(o) => itertools::Either::Left((v, o)),
        uploader::DownloadResult::Items(i) => itertools::Either::Right((v, i)),
    });
    let off = off.into_iter().into_group_map_by(|(v, _)| *v);
    let ext = ext.into_iter().into_group_map_by(|(v, _)| *v);
    let offers = off
        .into_iter()
        .map(|(k, v)| (k.clone(), v.into_iter().flat_map(|(_, v)| v).collect()))
        .collect();
    let external_items = ext
        .into_iter()
        .map(|(k, v)| (k.clone(), v.into_iter().flat_map(|(_, v)| v).collect()))
        .collect();
    Ok((offers, external_items))
}

#[derive(Debug, Display)]
pub enum ExportError {
    #[display("Unable to download items from link: {:?}", _0)]
    Download(String, uploader::DownloadFromLinkError),
    #[display("{}", _0)]
    Other(anyhow::Error),
}

impl From<anyhow::Error> for ExportError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<std::io::Error> for ExportError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

static SEMAPHORE: Semaphore = Semaphore::const_new(3);

fn ensure_bilingual(p: &mut Product) {
    let ua_title = p
        .ua_translation
        .as_ref()
        .map(|t| t.title.clone())
        .filter(|t| !t.is_empty());
    if p.title.is_empty() {
        if let Some(title) = ua_title.clone() {
            p.title = title;
        }
    }
    if ua_title.is_none() {
        let title = if p.title.is_empty() {
            String::new()
        } else {
            p.title.clone()
        };
        p.ua_translation.get_or_insert(UaTranslation {
            title,
            description: None,
        });
    }

    let desc_ru = p.description.clone().or_else(|| {
        p.ua_translation
            .as_ref()
            .and_then(|t| t.description.clone())
    });
    let desc_ua = p
        .ua_translation
        .as_ref()
        .and_then(|t| t.description.clone())
        .or_else(|| desc_ru.clone());

    p.description = desc_ru;
    if let Some(ua) = p.ua_translation.as_mut() {
        ua.description = desc_ua;
    } else {
        p.ua_translation = Some(UaTranslation {
            title: p.title.clone(),
            description: desc_ua,
        });
    }
}

pub async fn do_export(
    entry: &ExportEntry,
    shop: IdentityOf<Shop>,
    client: Client,
    shop_id: &str,
    dt_repo: Arc<dyn dt::product::ProductRepository>,
    tt_repo: Arc<dyn tt::product::ProductRepository>,
    davi_repo: Arc<dyn rt_parsing_davi::ProductRepository>,
    category_repo: Arc<dyn category::CategoryRepository>,
    trans_repo: Arc<dyn tt::product::TranslationRepository>,
    currency_service: Addr<CurrencyService>,
) -> Result<(), ExportError> {
    match tokio::fs::create_dir_all(format!("/tmp/export/{shop}")).await {
        Ok(_) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(err) => {
            log::error!("Unable to create temporal directory for shop {shop}: {err}");
        }
    }
    match tokio::fs::create_dir_all(format!("./export/{shop}")).await {
        Ok(_) => (),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(err) => {
            log::error!("Unable to create export directory for shop {shop}: {err}");
        }
    }
    let (dt, tt, davi, res) = tokio::join!(
        async {
            let need_dt = entry.dt_parsing.is_some()
                || entry.maxton_parsing.is_some()
                || entry.jgd_parsing.is_some()
                || entry.pl_parsing.is_some()
                || entry.skm_parsing.is_some()
                || entry.dt_tt_parsing.is_some();
            if !need_dt {
                return Ok::<Option<Vec<dt::product::Product>>, anyhow::Error>(None);
            }
            let wants_all = [
                entry.dt_parsing.as_ref().map(|x| x.options.only_available),
                entry
                    .maxton_parsing
                    .as_ref()
                    .map(|x| x.options.only_available),
                entry.jgd_parsing.as_ref().map(|x| x.options.only_available),
                entry.pl_parsing.as_ref().map(|x| x.options.only_available),
                entry.skm_parsing.as_ref().map(|x| x.options.only_available),
                entry
                    .dt_tt_parsing
                    .as_ref()
                    .map(|x| x.options.only_available),
            ]
            .into_iter()
            .flatten()
            .any(|v| !v);
            if wants_all {
                Ok(Some(dt_repo.list().await?))
            } else {
                Ok(Some(dt_repo.select(&dt::product::AvailableSelector).await?))
            }
        },
        async {
            let tt_only_available = entry
                .tt_parsing
                .as_ref()
                .map(|x| x.options.only_available)
                .or(entry
                    .dt_tt_parsing
                    .as_ref()
                    .map(|x| x.options.only_available));
            match &tt_only_available {
                Some(true) => Ok::<_, anyhow::Error>(Some(
                    tt_repo.select(&tt::product::AvailableSelector).await?,
                )),
                Some(false) => Ok(Some(tt_repo.list().await?)),
                None => Ok(None),
            }
        },
        async {
            match &entry.davi_parsing.as_ref().map(|x| x.only_available) {
                Some(true) => Ok::<_, anyhow::Error>(Some(
                    davi_repo
                        .select(&rt_types::product::AvailableSelector)
                        .await?,
                )),
                Some(false) => Ok(Some(davi_repo.list().await?)),
                None => Ok(None),
            }
        },
        async {
            match &entry.links {
                Some(l) => Some(parse_from_links(l, client.clone()).await),
                None => None,
            }
            .transpose()
            .map(Option::unzip)
        }
    );
    let (dt, tt, davi) = (dt?, tt?, davi?);
    let dt_articles: Option<HashSet<String>> = dt
        .as_ref()
        .map(|list| list.iter().map(|p| p.article.to_uppercase()).collect());
    let (offers, items) = res?;

    let mut res: HashMap<ExportOptions, Vec<Product>, _> =
        HashMap::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new());
    let categories = category_repo.select(&By(shop)).await?.into_iter().fold(
        HashSet::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new()),
        |mut r, e| {
            r.insert(e);
            r
        },
    );
    let categories_used = entry
        .dt_parsing
        .as_ref()
        .is_some_and(|opts| opts.options.categories)
        || entry
            .tt_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .maxton_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .jgd_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .pl_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .skm_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .dt_tt_parsing
            .as_ref()
            .is_some_and(|opts| opts.options.categories)
        || entry
            .davi_parsing
            .as_ref()
            .is_some_and(|opts| opts.categories)
        || items.as_ref().is_some_and(|i| {
            i.iter()
                .any(|(e, _)| e.options.as_ref().is_some_and(|opts| opts.categories))
        })
        || offers.as_ref().is_some_and(|i| {
            i.iter()
                .any(|(e, _)| e.options.as_ref().is_some_and(|opts| opts.categories))
        });
    let mut categories = if categories_used {
        categories
    } else {
        HashSet::with_hasher(xxhash_rust::xxh3::Xxh3DefaultBuilder::new())
    };
    let rates = match currency_service.send(ListRates).await {
        Ok(rates) => rates
            .into_iter()
            .map(|(k, v)| (k, v * dec!(1.07)))
            .collect(),
        Err(err) => {
            log::error!("Unable to list rates: {err}");
            HashMap::new()
        }
    };
    if let Some(offers) = offers {
        for (e, i) in offers {
            let vendor = e
                .vendor_name
                .unwrap_or_else(|| match parse_vendor_from_link(&e.link) {
                    Some(v) => v,
                    None => {
                        log::warn!("Unable to parse vendor from link {}", &e.link);
                        String::new()
                    }
                });
            let opts = e.options.unwrap_or_default();
            if i.is_empty() {
                log::warn!("Empty items list for offers: {opts:#?}");
            }
            let items =
                rt_types::product::convert(i.into_iter().map(Vendored::with_vendor(vendor)));
            let items: Vec<_> = if opts.categories {
                rt_types::category::assign_categories(items, &categories).collect()
            } else {
                items.collect()
            };
            let items = match opts.convert_to_uah {
                true => items
                    .into_iter()
                    .map(|mut i| {
                        if let Some(rate) = rates.get(&i.currency) {
                            i.currency = "UAH".to_string();
                            i.price *= rate;
                        }
                        i
                    })
                    .collect(),
                false => items,
            };
            let mut items: Vec<_> = items.into_iter().collect();
            items.iter_mut().for_each(ensure_bilingual);
            res.insert(opts, items);
        }
    }
    if let Some(items) = items {
        for (e, i) in items {
            let vendor = e
                .vendor_name
                .unwrap_or_else(|| match parse_vendor_from_link(&e.link) {
                    Some(v) => v,
                    None => {
                        log::warn!("Unable to parse vendor from link {}", &e.link);
                        String::new()
                    }
                });
            let opts = e.options.unwrap_or_default();
            if i.is_empty() {
                log::warn!("Empty items list for items: {opts:#?}");
            }
            let offers =
                rt_types::product::convert(i.into_iter().map(Vendored::with_vendor(vendor)));
            let offers: Vec<_> = if opts.categories {
                rt_types::category::assign_categories(offers, &categories).collect()
            } else {
                offers.collect()
            };
            let mut offers = match opts.convert_to_uah {
                true => offers
                    .into_iter()
                    .map(|mut i| {
                        if let Some(rate) = rates.get(&i.currency) {
                            i.currency = "UAH".to_string();
                            i.price *= rate;
                        }
                        i
                    })
                    .collect(),
                false => offers,
            };
            offers.iter_mut().for_each(ensure_bilingual);
            if let Some(entry) = res.get_mut(&opts) {
                entry.append(&mut offers);
            } else {
                res.insert(opts, offers);
            }
        }
    }
    if let Some((options, products)) = entry.tt_parsing.as_ref().zip(tt) {
        let products = match &options.append_categories {
            Some(ParsingCategoriesAction::BeforeTitle { separator }) => products
                .into_iter()
                .map(|mut p| {
                    if let Some(category) = &p.category {
                        p.title = format!("{} {} {}", category.trim(), separator.trim(), p.title);
                    }
                    p
                })
                .collect(),
            Some(ParsingCategoriesAction::AfterTitle { separator }) => products
                .into_iter()
                .map(|mut p| {
                    if let Some(category) = &p.category {
                        p.title = format!("{} {} {}", p.title, separator.trim(), category.trim());
                    }
                    p
                })
                .collect(),
            None => products,
        };
        let products: Vec<_> = stream::iter(products)
            .map(|mut p| async {
                let trans = trans_repo.get_one(&p.id).await?;
                if let Some(trans) = trans {
                    p.title = trans.title;
                    p.description = trans.description;
                    Ok::<_, anyhow::Error>(Some(p))
                } else {
                    Ok(None)
                }
            })
            .buffered(10)
            .filter_map(|p| async { p.transpose() })
            .try_collect()
            .await?;
        let products_for_dt_tt = products.clone();
        let dto = rt_types::product::convert(products.into_iter());
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        dto.iter_mut().for_each(ensure_bilingual);
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
        if let Some((dt_tt_opts, dt_articles)) =
            entry.dt_tt_parsing.as_ref().zip(dt_articles.as_ref())
        {
            let filtered: Vec<_> = products_for_dt_tt
                .into_iter()
                .filter(|p| p.available == Availability::Available)
                .filter(|p| dt_articles.contains(&p.article.to_uppercase()))
                .collect();
            let dto = rt_types::product::convert(filtered.into_iter());
            let dto: Vec<_> = if dt_tt_opts.options.categories {
                category::assign_categories(dto, &categories).collect()
            } else {
                dto.collect()
            };
            let mut dto = match dt_tt_opts.options.convert_to_uah {
                true => dto
                    .into_iter()
                    .map(|mut i| {
                        if let Some(rate) = rates.get(&i.currency) {
                            i.currency = "UAH".to_string();
                            i.price *= rate;
                        }
                        i
                    })
                    .collect(),
                false => dto,
            };
            dto.iter_mut().for_each(ensure_bilingual);
            if let Some(entry) = res.get_mut(&dt_tt_opts.options) {
                entry.append(&mut dto);
            } else {
                res.insert(dt_tt_opts.options.clone(), dto);
            }
        }
    }
    let (maxton, jgd, pl, skm, dt): (
        Option<Vec<_>>,
        Option<Vec<_>>,
        Option<Vec<_>>,
        Option<Vec<_>>,
        Option<Vec<_>>,
    ) = dt
        .map(|dt| {
            let (maxton, rest): (Vec<_>, Vec<_>) = dt.into_iter().partition(|p| {
                p.article.ends_with("-M")
                    || p.title.to_lowercase().contains("maxton")
                    || p.description
                        .as_ref()
                        .is_some_and(|d| d.to_lowercase().contains("maxton"))
            });
            let (jgd, rest): (Vec<_>, Vec<_>) = rest
                .into_iter()
                .partition(|p| p.article.to_uppercase().starts_with("JGD"));
            let (pl, dt): (Vec<_>, Vec<_>) = rest
                .into_iter()
                .partition(|p| PL_ARTICLES.contains(p.article.to_uppercase().as_str()));
            let (skm, dt): (Vec<_>, Vec<_>) = dt
                .into_iter()
                .partition(|p| p.article.to_uppercase().starts_with("SKM"));
            (Some(maxton), Some(jgd), Some(pl), Some(skm), Some(dt))
        })
        .unwrap_or((None, None, None, None, None));
    if let Some((options, products)) = entry.dt_parsing.as_ref().zip(dt) {
        // DT items should always carry vendor info into notes, even if the flag
        // was not set explicitly.
        let mut opts = options.options.clone();
        opts.add_vendor = true;
        let dto = rt_types::product::convert(products.into_iter());
        let dto: Vec<_> = if opts.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match opts.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        if let Some(entry) = res.get_mut(&opts) {
            entry.append(&mut dto);
        } else {
            res.insert(opts, dto);
        }
    }
    if let Some((options, products)) = entry.jgd_parsing.as_ref().zip(jgd) {
        let products = products.into_iter().map(|mut p| {
            p.available = Availability::OnOrder;
            p
        });
        let dto = rt_types::product::convert(products);
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        dto.iter_mut().for_each(|p| {
            p.vendor = "JGD".to_string();
            ensure_bilingual(p);
        });
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    if let Some((options, products)) = entry.pl_parsing.as_ref().zip(pl) {
        let products = products.into_iter().map(|mut p| {
            p.available = Availability::OnOrder;
            p
        });
        let dto = rt_types::product::convert(products);
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        dto.iter_mut().for_each(|p| {
            p.vendor = "Скловолокно PL".to_string();
            ensure_bilingual(p);
        });
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    if let Some((options, products)) = entry.skm_parsing.as_ref().zip(skm) {
        let products = products.into_iter().map(|mut p| {
            p.available = Availability::OnOrder;
            p
        });
        let dto = rt_types::product::convert(products);
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        dto.iter_mut().for_each(|p| {
            p.vendor = "SKM".to_string();
            ensure_bilingual(p);
        });
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    if let Some((options, products)) = entry.maxton_parsing.as_ref().zip(maxton) {
        let mut skipped_t_suffix = 0usize;
        let filtered: Vec<_> = products
            .into_iter()
            .filter(|p| {
                let drop = p.article.ends_with('T');
                if drop {
                    skipped_t_suffix += 1;
                }
                !drop
            })
            .collect();
        if skipped_t_suffix > 0 {
            log::warn!(
                "Maxton: skipped {skipped_t_suffix} items with trailing 'T' suffix (unsupported on supplier site)"
            );
        }
        let dto = rt_types::product::convert(filtered.into_iter().map(dt::product::MaxtonProduct))
            .map(|mut p| {
                p.available = Availability::OnOrder;
                p
            });
        let dto: Vec<_> = if options.options.categories {
            category::assign_categories(dto, &categories).collect()
        } else {
            dto.collect()
        };
        let mut dto = match options.options.convert_to_uah {
            true => dto
                .into_iter()
                .map(|mut i| {
                    if let Some(rate) = rates.get(&i.currency) {
                        i.currency = "UAH".to_string();
                        i.price *= rate;
                    }
                    i
                })
                .collect(),
            false => dto,
        };
        if let Some(entry) = res.get_mut(&options.options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.options.clone(), dto);
        }
    }
    if let Some((options, dto)) = entry.davi_parsing.as_ref().zip(davi) {
        let new_categories = rt_parsing_davi::get_categories(&dto, categories.clone(), shop);
        for c in new_categories.iter() {
            category_repo
                .save(c.clone())
                .await
                .log_error("Unable to save new category for davi product");
        }
        categories = new_categories;
        let dto: Vec<_> = dto
            .into_iter()
            .map(|d: rt_parsing_davi::Product| d.enrich(categories.iter()))
            .collect();
        let mut dto = dto.into_iter().map(Into::into).collect();
        if let Some(entry) = res.get_mut(&options) {
            entry.append(&mut dto);
        } else {
            res.insert(options.clone(), dto);
        }
    }
    let instant = std::time::Instant::now();

    let count: usize = res.values().map(|v| v.len()).sum();

    let file_path = |file_name| format!("/tmp/export/{shop}/{file_name}");
    let c = categories.clone();
    let xlsx_filename = file_path(entry.file_name(FileFormat::Xlsx));
    let f = xlsx_filename.clone();
    let i = std::time::Instant::now();
    let id = shop_id.to_string();

    let res = rt_types::watermark::apply_to_product_map(res, shop_id, &Lazy::force(&SELF_ADDR))?;
    let res: HashMap<_, _> = res
        .into_iter()
        .map(|(o, p)| {
            let only_available = o.only_available;
            let p = p.into_iter().filter(|p| {
                if only_available {
                    if let Availability::Available | Availability::OnOrder = p.available {
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            });
            if let Some(av) = &o.set_availability {
                let p = p
                    .map(|mut p| {
                        p.available = av.clone();
                        p
                    })
                    .collect();
                (o, p)
            } else {
                (o, p.collect())
            }
        })
        .collect();
    let r = res.clone();

    tokio::task::spawn_blocking(move || {
        crate::xlsx::write_xlsx_dto_map(&f, r, c, &id)?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("Unable to join thread")??;

    let xlsx = i.elapsed().as_millis();

    let xml_filename = format!("{}", file_path(entry.file_name(FileFormat::Xml)));
    let i = std::time::Instant::now();
    crate::xml::write_dto_map(&xml_filename, &res, categories, shop_id).await?;
    let xml = i.elapsed().as_millis();

    let horoshop_filename = format!("{}", file_path(entry.file_name(FileFormat::HoroshopCsv)));
    let i = std::time::Instant::now();
    crate::horoshop::export_csv(horoshop_filename.clone(), &res, category_repo.clone()).await?;

    let horoshop_filename = format!(
        "{}",
        file_path(entry.file_name(FileFormat::HoroshopCategories))
    );
    crate::horoshop::export_csv_categories(horoshop_filename.clone(), &res, category_repo.clone())
        .await?;
    let horoshop = i.elapsed().as_millis();
    let i = std::time::Instant::now();
    let csv_filename = format!("{}", file_path(entry.file_name(FileFormat::Csv)));
    crate::csv::write_dto_map(&csv_filename, res, shop_id).await?;
    log::info!(
        "Time:\nxlsx: {xlsx}ms\nxml: {xml}ms\ncsv: {}ms\nhoroshop: {horoshop}ms",
        i.elapsed().as_millis()
    );
    log::info!(
        "Performance: {} items/min",
        (count as f32 / instant.elapsed().as_millis() as f32) * 1000. * 60.
    );

    let res = tokio::join!(
        tokio::fs::copy(&xlsx_filename, xlsx_filename.replace("/tmp", ".")),
        tokio::fs::copy(&xml_filename, xml_filename.replace("/tmp", ".")),
        tokio::fs::copy(&csv_filename, csv_filename.replace("/tmp", ".")),
        tokio::fs::copy(&horoshop_filename, horoshop_filename.replace("/tmp", ".")),
    );
    res.0?;
    res.1?;
    res.2?;

    let (a, b, c, d) = tokio::join!(
        tokio::fs::remove_file(&xlsx_filename),
        tokio::fs::remove_file(&xml_filename),
        tokio::fs::remove_file(&csv_filename),
        tokio::fs::remove_file(&horoshop_filename),
    );
    let res = a
        .context(xlsx_filename)
        .and(b.context(xml_filename))
        .and(c.context(csv_filename))
        .and(d.context(horoshop_filename));
    if let Err(err) = res {
        log::error!("Unable to remove tmp file: {err}");
    }
    Ok(())
}
