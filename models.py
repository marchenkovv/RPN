import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True, slots=True)
class PatientRecord:
    """Запись пациента для сверки."""
    enp: str
    bp: str
    fam: str
    im: str
    ot: str
    dr: str

    @property
    def full_key(self) -> Tuple[str, ...]:
        return self.enp, self.bp, self.fam, self.im, self.ot, self.dr

    @property
    def short_key(self) -> Tuple[str, str]:
        return self.enp, self.bp

    @classmethod
    def from_xml(cls, zap: ET.Element) -> 'PatientRecord':
        pacient = zap.find('PACIENT')
        return cls(
            enp=zap.findtext('ENP', ''),
            bp=zap.findtext('BP', ''),
            fam=pacient.findtext('FAM', '') if pacient is not None else '',
            im=pacient.findtext('IM', '') if pacient is not None else '',
            ot=pacient.findtext('OT', '') if pacient is not None else '',
            dr=pacient.findtext('DR', '') if pacient is not None else '',
        )

    @property
    def is_valid(self) -> bool:
        return bool(self.enp and self.bp)
