import re

with open('src/app/(workspace)/campaigns/[campaignId]/page.tsx', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Remove Logs panel from Activity Tab
# First, let's identify the Activity Tab block
logs_panel_pattern = r"\{/\* --- Logs Panel ---------------------------------------- \*/\}.*?(?=\{/\* Stats row \*/\})"
logs_panel_match = re.search(logs_panel_pattern, text, re.DOTALL)
logs_panel_code = logs_panel_match.group(0)

# Replace Activity Tab to remove Captcha and Form details
activity_table_old = '''                  <tr>
                    <th></th>
                    <th>Company</th>
                    <th>Contact URL</th>
                    <th>Status</th>
                    <th>Captcha Found</th>
                    <th>Captcha Solved</th>
                    <th>Site Key Not Found</th>
                    <th>Form Found</th>
                    <th>Interested</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {activityRows.length === 0 ? (
                    <tr><td colSpan={10} className="table-empty">No results match current filter.</td></tr>
                  ) : (
                    activityRows.map((contact) => {
                      const result = getContactResult(contact);
                      const captcha = parseCaptchaStatus(result?.captchaStatus || "");
                      const _conf = (result?.confirmationMsg || "").toLowerCase();
                      const _cap = (result?.captchaStatus || "").toLowerCase();
                      const _negatives = ["not found", "not filled", "could not find", "no form", "form not", "unable to find", "failed to find", "no contact"];
                      const _capFoundPositive = _cap.includes("found") && !_negatives.some(n => _cap.includes(n));
                      const _confFormPositive = (_conf.includes("form") || _conf.includes("submitted") || _conf.includes("sent") || _conf.includes("message")) && !_negatives.some(n => _conf.includes(n));
                      const formFound = _capFoundPositive || _confFormPositive;
                      return (
                        <tr key={contact.id} className={result?.status === "success" ? "bg-green-50/30" : result?.status === "fail" ? "bg-red-50/30" : ""}>
                          <td><StatusIcon status={result?.status ?? null} /></td>
                          <td
                            className="font-medium text-blue-700 cursor-pointer hover:underline hover:text-blue-900 transition-colors"
                            onClick={() => setSelectedDetail({ contact, result: result ?? null })}
                            title="Click for full details"
                          >
                            {contact.companyName}
                          </td>
                          <td>
                            <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link flex items-center gap-1">
                              <span className="truncate max-w-[160px] inline-block">{contact.contactUrl}</span>
                              <ExternalLink size={11} />
                            </a>
                          </td>
                          <td>
                            <span className={+\\	ext-xs font-medium px-2 py-0.5 rounded-full +\\}>
                              {result?.status ?? "pending"}
                            </span>
                          </td>
                          <td className="text-center">{result ? (captcha.found ? "?" : "?") : "—"}</td>
                          <td className="text-center">{result ? (captcha.solved ? "?" : "?") : "—"}</td>
                          <td className="text-center">{result ? (captcha.siteKeyNotFound ? "??" : "—") : "—"}</td>
                          <td className="text-center">{result ? (formFound ? "?" : "?") : "—"}</td>
                          <td>
                            <button
                              type="button"
                              onClick={() => void toggleInterested(contact)}
                              disabled={togglingContactId === contact.id}
                              className={+\\w-5 h-5 rounded border-2 flex items-center justify-center transition-colors +\\}
                            >
                              {contact.isInterested && <Heart size={10} fill="currentColor" />}
                            </button>
                          </td>
                          <td className="text-xs text-gray-400 max-w-[180px] truncate" title={result?.confirmationMsg}>
                            <button
                              type="button"
                              className="text-xs text-blue-600 hover:underline"
                              onClick={() => setSelectedDetail({ contact, result: result ?? null })}
                            >
                              {result ? "View Details" : "—"}
                            </button>
                          </td>
                        </tr>
                      );
                    })'''

activity_table_new = '''                  <tr>
                    <th></th>
                    <th>Company</th>
                    <th>Contact URL</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {activityRows.length === 0 ? (
                    <tr><td colSpan={4} className="table-empty">No results match current filter.</td></tr>
                  ) : (
                    activityRows.map((contact) => {
                      const result = getContactResult(contact);
                      return (
                        <tr key={contact.id} className={result?.status === "success" ? "bg-green-50/30" : result?.status === "fail" ? "bg-red-50/30" : ""}>
                          <td><StatusIcon status={result?.status ?? null} /></td>
                          <td className="font-medium text-gray-800">
                            {contact.companyName}
                          </td>
                          <td>
                            <a href={contact.contactUrl} target="_blank" rel="noreferrer" className="table-link flex items-center gap-1">
                              <span className="truncate max-w-[160px] inline-block">{contact.contactUrl}</span>
                              <ExternalLink size={11} />
                            </a>
                          </td>
                          <td>
                            <span className={+\\	ext-xs font-medium px-2 py-0.5 rounded-full +\\}>
                              {result?.status === "success" ? "Site successfully submit" : result?.status === "fail" ? "Fail" : result?.status ?? "pending"}
                            </span>
                          </td>
                        </tr>
                      );
                    })'''

# Replace logs panel with empty from Activity Tab
text = text.replace(logs_panel_code, "")
text = text.replace(activity_table_old, activity_table_new)

# Now, we define Results Tab string. It will include the logs panel and the old detailed table.
results_tab = f"""
      {{/* --- RESULTS TAB ---------------------------------------- */}}
      {{activeTab === "results" && (
        <section className="panel" style={{{{ borderTopLeftRadius: 0 }}}}>
{logs_panel_code}

          {{!runSnapshot && (
            <div className="empty-state">
              <Database size={{48}} strokeWidth={{1}} />
              <h3>No detailed results</h3>
              <p>Start a campaign run to see comprehensive logs and failure reasons.</p>
            </div>
          )}}

          {{runSnapshot && (
            <div className="table-wrap">
              <table className="clean-table">
                <thead>{activity_table_old.split('<thead>')[1]}
                  )}}
                </tbody>
              </table>
            </div>
          )}}
        </section>
      )}}
"""

editor_tab = """
      {/* --- EDITOR TAB ----------------------------------------- */}
      {activeTab === "editor" && (
        <section className="panel" style={{ borderTopLeftRadius: 0 }}>
          <div className="flex justify-between items-center mb-4">
            <div>
              <h3 className="font-semibold text-gray-800">Campaign Editor</h3>
              <p className="text-sm text-gray-500">Configure AI steps and submission schedules.</p>
            </div>
            <button
              type="button"
              onClick={() => setStepsLocal(prev => [...prev, { id: Date.now().toString(), aiInstruction: "", daySequence: 1, timeOfDay: "09:00", type: "immediate", enabled: true }])}
              className="flex items-center gap-2 px-3 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm font-medium transition-colors"
            >
              <Plus size={15} /> Add Step
            </button>
          </div>

          <div className="space-y-4">
            {stepsLocal.length === 0 ? (
              <div className="p-8 text-center border-2 border-dashed border-gray-200 rounded-xl bg-gray-50">
                <Terminal size={32} className="mx-auto text-gray-400 mb-2" />
                <p className="text-gray-600 font-medium text-sm">No follow-up steps configured.</p>
                <p className="text-gray-400 text-xs mt-1">Add a step to configure automated follow-up messages.</p>
              </div>
            ) : (
              stepsLocal.map((step, i) => (
                <div key={step.id || i} className={p-4 border  rounded-xl shadow-sm transition-all}>
                  <div className="flex flex-wrap gap-4 items-start pb-4 border-b border-gray-100 mb-4">
                    <div className="w-8 h-8 rounded-full bg-blue-100 text-blue-700 text-sm font-bold flex items-center justify-center shrink-0">
                      {i + 1}
                    </div>
                    
                    <div className="flex-1 min-w-[200px] flex gap-4 flex-wrap">
                      <label className="flex-1 min-w-[120px]">
                        <span className="block text-xs font-semibold text-gray-500 mb-1 uppercase tracking-wider">Type</span>
                        <select
                          value={step.type || "immediate"}
                          onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, type: e.target.value } : s))}
                          className="field-input text-sm py-1.5"
                        >
                          <option value="immediate">Immediate Send</option>
                          <option value="normal">Scheduled Send</option>
                        </select>
                      </label>
                      
                      {step.type === "normal" && (
                        <>
                          <label className="w-24">
                            <span className="block text-xs font-semibold text-gray-500 mb-1 uppercase tracking-wider">Day +</span>
                            <input
                              type="number"
                              min={1}
                              value={step.daySequence || 1}
                              onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, daySequence: Number(e.target.value) } : s))}
                              className="field-input text-sm py-1.5"
                            />
                          </label>
                          <label className="w-32">
                            <span className="block text-xs font-semibold text-gray-500 mb-1 uppercase tracking-wider">Time</span>
                            <div className="relative">
                              <Clock size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
                              <input
                                type="time"
                                value={step.timeOfDay || "09:00"}
                                onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, timeOfDay: e.target.value } : s))}
                                className="field-input text-sm py-1.5 pl-8"
                              />
                            </div>
                          </label>
                        </>
                      )}
                    </div>
                    
                    <div className="flex items-center gap-3 shrink-0 mt-1">
                      <button
                        type="button"
                        onClick={() => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, enabled: !s.enabled } : s))}
                        className={	ext-xs font-medium px-3 py-1.5 rounded-md transition-colors }
                      >
                        {step.enabled ? "ON" : "OFF"}
                      </button>
                      <button
                        type="button"
                        onClick={() => setStepsLocal(prev => prev.filter((_, idx) => idx !== i))}
                        className="p-1.5 text-red-400 hover:text-red-700 hover:bg-red-50 rounded-md transition-colors"
                      >
                        <Trash2 size={16} />
                      </button>
                    </div>
                  </div>
                  
                  <div className="px-2">
                    <span className="block text-xs font-semibold text-gray-500 mb-2 uppercase tracking-wider">AI Instruction</span>
                    <textarea
                      value={step.aiInstruction || (typeof step === 'string' ? step : "")}
                      onChange={e => setStepsLocal(prev => prev.map((s, idx) => idx === i ? { ...s, aiInstruction: e.target.value } : s))}
                      rows={4}
                      className="field-input field-textarea text-sm w-full"
                      placeholder="What should the AI do in this step?"
                    />
                  </div>
                </div>
              ))
            )}
            
            <div className="pt-4 border-t border-gray-100 flex justify-end">
              <button
                type="button"
                onClick={() => void saveSteps()}
                disabled={savingSteps}
                className="px-6 py-2.5 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 text-white text-sm font-medium rounded-lg transition-colors"
              >
                {savingSteps ? "Saving..." : "Save Steps"}
              </button>
            </div>
          </div>
        </section>
      )}
"""

# Insert Results and Editor tabs right before Settings Tab
settings_tab_anchor = "{/* --- SETTINGS TAB --------------------------------------- */}"
text = text.replace(settings_tab_anchor, results_tab + editor_tab + settings_tab_anchor)

with open('src/app/(workspace)/campaigns/[campaignId]/page.tsx', 'w', encoding='utf-8') as f:
    f.write(text)

print("Tabs successfully updated.")
