import json
import datetime

class ResourceSlot:
    def __init__(self, start_time, resources, end_time=None):
        self.start_time = start_time
        self.end_time = end_time
        self.resources = resources

    def to_map(self):
        s = {'start_time': self.start_time.isoformat(),
             'resources': self.resources}
        if self.end_time is not None:
            s['end_time'] = self.end_time
        return s
        
    def from_map(cls, executionslot_map):
        end_time = None
        if 'end_time' in resourceslot_map:
            end_time = resourceslot_map['end_time']
        s = ResourceSlot(datetime.datetime.fromisoformat(executionslot_map['start_time']),
                         executionslot_map['resources'],
                         end_time=end_time)
        return s

    def __repr__(self):
        return "ResourceSlot(start_time{}, resources={}, end_time={})".format(
            repr(self.start_time), repr(self.resources), repr(self.end_time)
        )
        
    def get_duration(self):
        if self.end_time is None:
            return None
        else:
            return self.end_time - self.start_time
        
class ExecutionSlot(ResourceSlot):
    def __init__(self, start_time, end_time, proportion_run, resources):
        self.proportion_run = proportion_run
        super().__init__(start_time, resources, end_time)
        
    def to_map(self):
        return {'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat(),
                'proportion_run': self.proportion_run,
                'resources': self.resources}
    
    @classmethod
    def from_map(cls, executionslot_map):
        s = ExecutionSlot(datetime.datetime.fromisoformat(executionslot_map['start_time']),
                          datetime.datetime.fromisoformat(executionslot_map['end_time']),
                          executionslot_map['proportion_run'],
                          executionslot_map['resources'])
        return s

        
    def __repr__(self):
        return "ExecutionSlot(start_time{}, end_time={}, proportion_run={}, percent_run resources={}, )".format(
            repr(self.start_time), repr(self.end_time), self.proportion_run, repr(self.resources)
        )

class Task:
    def __init__(self, name, duration={'seconds': 0}, required_resources={}, value=0.0, can_split=False):
        self.name = name
        self.duration_definition = duration
        self.required_resources = required_resources
        self.value = value
        self.can_split = can_split

        self.depends_on_tasks = []
        self.dependent_tasks = []
        self.parent = None
        self.children = []

        self.start_time = None
        self.end_time = None

        self.run_slots = []
        self.proportion_run_total = 0.0
        
    def to_map(self, run_data=True):
        task_map = {'name': self.name,
                    'required_resources': self.required_resources}
        if self.can_split:
            task_map['can_split'] = self.can_split
        if self.value != 0.0:
            task_map['value'] = self.value
        if self.duration_definition != {'seconds': 0}:
            task_map['duration'] = self.duration_definition
        if self.parent is not None:
            task_map['parent'] = self.parent.name
        if len(self.depends_on_tasks):
            task_map['depends_on_tasks'] = [t.name for t in self.depends_on_tasks]
        if run_data:
            if self.start_time is not None:
                task_map['start_time'] = self.start_time.isoformat()
            if self.end_time is not None:
                task_map['end_time'] = self.end_time.isoformat()
            task_map['proportion_run_total'] = self.proportion_run_total
            task_map['run_slots'] = [s.to_map() for s in self.run_slots]
        return task_map
            
    @classmethod
    def from_map(cls, task_map):
        duration = {'seconds': 0}
        if 'duration' in task_map:
            duration = task_map['duration']
        value = 0.0
        if 'value' in task_map:
            value = task_map['value']
        can_split = False
        if 'can_split' in task_map:
            can_split = task_map['can_split']
        t = Task(task_map['name'], duration=duration,
                 required_resources=task_map['required_resources'],
                 value=value,
                 can_split=can_split)
        if 'start_time' in task_map:
            t.start_time = datetime.datetime.fromisoformat(task_map['start_time'])
        if 'end_time' in task_map:
            t.end_time = datetime.datetime.fromisoformat(task_map['end_time'])
        if 'proportion_run_total' in task_map:
            t.proportion_run_total = task_map['proportion_run_total']
        if 'run_slots' in task_map:
            t.run_slots = [ExecutionSlot.from_map(e) for e in task_map['run_slots']]
        return t
        
        
    def __repr__(self):
        return "Task(name={}, duration_definition={}, required_resources={}, value={})".format(
            repr(self.name),
            repr(self.duration_definition),
            repr(self.required_resources),
            repr(self.value)
        )
    
    def __str__(self):
        return "Task({}, started={}, ended={}, duration={}, value={})".format(
            self.name,
            "NA" if self.start_time is None else str(self.start_time),
            "NA" if self.end_time is None else str(self.end_time),
            "NA" if self.end_time is None else str(self.end_time - self.start_time),
            self.value
        )
    
    def duration_from_resources(self, resources):
        globals().update(resources)
        timedelta = {c: eval(f)
                     for c, f in self.duration_definition.items()
                     if isinstance(f, str)}
        timedelta.update(
            {c: f
             for c, f in self.duration_definition.items()
             if not isinstance(f, str)}
        )
        duration = datetime.timedelta(**timedelta)
        for r in resources:
            del globals()[r]
        return duration
    
    def add_child(self, child):
        if child not in self.children:
            self.children.append(child)
        child.parent = self
        
    def add_dependent(self, dependent):
        if dependent not in self.dependent_tasks:
            self.dependent_tasks.append(dependent)
        if self not in dependent.depends_on_tasks:
            dependent.depends_on_tasks.append(self)
            
    def get_value(self):
        value = self.value
        value += sum((c.get_value() for c in self.children))
        value += sum((d.get_value() for d in self.dependent_tasks))
        return value

    def can_start(self):
        can_start = all([t.end_time is not None for t in self.depends_on_tasks])
        if self.parent is not None:
            can_start &= self.parent.start_time is not None
        return can_start
    
    def resources_meet_minimum(self, resources):
        resources_ok = all(resources[r] >= self.required_resources[r]
                           for r in self.required_resources)
        return resources_ok
            
    def get_earliest_start(self, schedule_start_time):
        if not self.can_start():
            return None
        start_blockers = [schedule_start_time]
        if self.parent is not None:
            start_blockers.append(self.parent.start_time)
        start_blockers.extend([d.end_time for d in self.depends_on_tasks])
        return max(start_blockers)
    
    def get_earliest_end(self):
        if self.start_time is None:
            return None
        return max([self.start_time] + [c.end_time for c in self.children])
    
    def can_end(self):
        return self.can_start() and all(c.end_time is not None for c in self.children)
        
    def get_sensitive_resources(self):
        return [
            r for c in self.duration_definition.values()
            for r in self.required_resources
            if isinstance(c, str) and r in c
        ]
    
    def get_optimized_resources(self, resources):
        resources_used = {r: resources[r] for r in resources if r in self.required_resources}
        for r in resources_used:
            if r not in self.get_sensitive_resources():
                resources_used[r] = self.required_resources[r]
        optimal_time = self.duration_from_resources(resources)
        for resource in self.get_sensitive_resources():
            while resources_used[resource] != self.required_resources[resource]:
                test_resources = {r: resources_used[r] for r in resources_used}
                resources_used[resource] -= 1
                if self.duration_from_resources(resources_used) > optimal_time:
                    resources_used[resource] += 1
                    break
        return resources_used
    
    def schedule(self, resource_slots):
        zero_duration = self.duration_from_resources(self.required_resources).total_seconds() == 0
        pure_parent_task = zero_duration and len(self.children)
        if self.proportion_run_total == 1.0 or pure_parent_task:
            self.run_slots
        
        run_slots = self.run_slots.copy()
        proportion_run_total = self.proportion_run_total

        schedule_start = min(s.start_time for s in resource_slots)
        def slot_valid(slot):
            time_valid = slot.start_time >= self.get_earliest_start(schedule_start)
            not_used = slot.start_time >= max([schedule_start] + [s.start_time for s in self.run_slots])
            resource_valid = self.resources_meet_minimum(slot.resources)
            return time_valid and resource_valid
        usable_slots = filter(slot_valid, resource_slots)

        for slot in usable_slots:
            proportion_runnable = 0.0
            if slot.get_duration() is None:
                proportion_runnable = 1.0
            else:
                proportion_runnable = 1.0
                if not zero_duration:
                    proportion_runnable = slot.get_duration() / self.duration_from_resources(slot.resources)
                
            if self.can_split or proportion_runnable >= 1.0:
                candidate_durations = [self.duration_from_resources(slot.resources)]
                if slot.get_duration() is not None:
                    candidate_durations.append(slot.get_duration())
                period_runnable = min(candidate_durations)
                run_slots.append(
                    ExecutionSlot(
                        slot.start_time,
                        slot.start_time + period_runnable,
                        proportion_runnable,
                        self.get_optimized_resources(slot.resources)
                    )
                )
                proportion_run_total += max(proportion_runnable, 1 - proportion_run_total)
                if proportion_run_total == 1.0:
                    break

        return run_slots
    
class TaskScheduler:
    def __init__(self, start_time, resources, tasks):
        self.resources = resources
        self.start_time = start_time
        self.tasks = tasks
    
    def schedule(self):
        while any((task.end_time is None for task in self.tasks)):
            self.start_next_task()
            while self.stop_tasks() != 0:
                pass

    def tasks_by_value(self):
        return sorted(self.tasks, key=lambda t: t.get_value(), reverse=True)
    
    def stop_tasks(self):
        stopable_tasks = filter(lambda t: t.end_time is None and t.can_end(),
                                self.tasks_by_value())
        tasks_stopped = 0
        for task in stopable_tasks:
            self.stop_task(task)
            tasks_stopped += 1
        return tasks_stopped
        
    def stop_task(self, task):
        resource_schedule = self.get_resource_slots()
        task.run_slots = task.schedule(resource_schedule)
        task.proportion_run_total = 1.0
        if task.start_time is None:
            task.start_time = min([run_slot.start_time for run_slot in task.run_slots])
        task.end_time = max([run_slot.end_time for run_slot in task.run_slots] + [task.get_earliest_end()])
    
    def start_next_task(self):
        resource_schedule = self.get_resource_slots()
        def startable(task):
            return task.end_time is None and task.can_start()
        tasks_to_start = filter(startable,
                                self.tasks_by_value())
        candidates = [next(tasks_to_start)]
        candidates.extend(filter(lambda t: t.get_value() == candidates[0].get_value(),
                          tasks_to_start))
        task = min(
            map(lambda c: (c, 
                           max(c.schedule(resource_schedule), key=lambda s: s.end_time)),
                candidates),
            key=lambda s: s[1])[0]
        if task.can_end():
            self.stop_task(task)
        else:
            task.start_time = task.get_earliest_start(self.start_time)
    
    def get_slot_overlapping_slots(self, slot, slots):
        overlapping_slots = []
        for test_slot in slots:
            start_overlap = slot.start_time >= test_slot.start_time
            end_overlap = True
            if test_slot.end_time is not None:
                end_overlap = slot.start_time < test_slot.end_time

            if start_overlap and end_overlap:
                overlapping_slots.append(test_slot)

        return overlapping_slots
    
    def split_slots(self, run_slot, overlapping_slots):
        slots_split = []
        for resource_slot in overlapping_slots:
            starts_mid = run_slot.start_time > resource_slot.start_time
            ends_mid = resource_slot.end_time is None or run_slot.end_time < resource_slot.end_time
            if starts_mid and ends_mid:
                slots_split.append(ResourceSlot(resource_slot.start_time, 
                                                resource_slot.resources,
                                                end_time=run_slot.start_time))
                slots_split.append(ResourceSlot(run_slot.start_time,
                                                self.add_resources(run_slot.resources, resource_slot.resources),
                                                end_time=run_slot.end_time))
                slots_split.append(ResourceSlot(run_slot.end_time,
                                                resource_slot.resources,
                                                end_time=resource_slot.end_time))
            elif starts_mid:
                slots_split.append(ResourceSlot(resource_slot.start_time,
                                                resource_slot.resources,
                                                end_time=run_slot.start_time))
                slots_split.append(ResourceSlot(run_slot.start_time,
                                                self.add_resources(run_slot.resources, resource_slot.resources),
                                                end_time=resource_slot.end_time))
            elif ends_mid:
                slots_split.append(ResourceSlot(resource_slot.start_time,
                                                self.add_resources(run_slot.resources, resource_slot.resources),
                                                end_time=run_slot.end_time))
                slots_split.append(ResourceSlot(run_slot.end_time,
                                                resource_slot.resources,
                                                end_time=resource_slot.end_time))
            else: #Covers entire slot
                slots_split.append(ResourceSlot(resource_slot.start_time,
                                                self.add_resources(run_slot.resources, resource_slot.resources),
                                                end_time=resource_slot.end_time))

        return slots_split
                
    def add_resources(self, resources_a, resources_b):
        resources_summed = {r: v for r, v in resources_a.items()}
        for r, v in resources_b.items():
            if r not in resources_summed:
                resources_summed[r] = v
            else:
                resources_summed[r] += v
        return resources_summed
    
    def subtract_resources(self, resources_a, resources_b):
        resources_b_negative = {r: -v for r, v in resources_b.items()}
        return self.add_resources(resources_a, resources_b_negative)
    
    def get_resource_slots(self):
        resource_slots = [ResourceSlot(self.start_time, {r: 0 for r in self.resources})]
        for task in self.tasks:
            for run_slot in task.run_slots:
                overlapping_slots = self.get_slot_overlapping_slots(run_slot, resource_slots)
                new_slots = self.split_slots(run_slot, overlapping_slots)
                for slot in overlapping_slots:
                    resource_slots.remove(slot)
                resource_slots.extend(new_slots)
                resource_slots.sort(key=lambda s: s.start_time)

        for slot in resource_slots:
            slot.resources = self.subtract_resources(self.resources, slot.resources)
            
        return resource_slots
    
def tasks_from_map(tasks_map):
    tasks = {t['name']: Task.from_map(t) for t in tasks_map}
    for t in tasks_map:
        if 'parent' in t:
            tasks[t['parent']].add_child(tasks[t['name']])
        if 'depends_on_tasks' in t:
            for r in t['depends_on_tasks']:
                tasks[r].add_dependent(tasks[t['name']])

    return list(tasks.values())